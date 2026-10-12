// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use serde::{Deserialize, Serialize};
use std::fmt;

/// A version range.
///
/// A range consists of two 16-bit numbers: the lowest version which is accepted, and the highest.
/// Ranges are inclusive, meaning that both the lowest and the highest version are valid versions.
/// The only exception to this is the NONE range, which contains no versions at all.
///
/// Version ranges can be represented as strings.
///
/// A single supported version V is represented as "V".
/// A bounded range from A to B is represented as "A-B".
/// All versions greater than A is represented as "A+".
/// The NONE range is represented as the string "none".
///
/// Translated from org.apache.kafka.message.Versions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Versions {
    lowest: i16,
    highest: i16,
}

impl Versions {
    /// The string representation of the NONE range.
    pub const NONE_STRING: &'static str = "none";

    /// A range representing all versions.
    pub const ALL: Versions = Versions { lowest: 0, highest: i16::MAX };

    /// A range representing no versions.
    pub const NONE: Versions = Versions { lowest: 0, highest: -1 };

    /// Parses a version range from a string.
    ///
    /// # Arguments
    /// * `input` - The string to parse, or None to return default_versions
    /// * `default_versions` - The default value to use if input is None or empty
    ///
    /// # Returns
    /// A Versions object representing the parsed range
    pub fn parse(input: Option<&str>, default_versions: Versions) -> Result<Versions, String> {
        let input = match input {
            None => return Ok(default_versions),
            Some(s) => s.trim(),
        };

        if input.is_empty() {
            return Ok(default_versions);
        }

        if input == Self::NONE_STRING {
            return Ok(Self::NONE);
        }

        if let Some(version_str) = input.strip_suffix('+') {
            let lowest = version_str
                .parse::<i16>()
                .map_err(|e| format!("Failed to parse version: {}", e))?;
            return Versions::new(lowest, i16::MAX);
        }

        if let Some(dash_index) = input.find('-') {
            let lowest = input[..dash_index]
                .parse::<i16>()
                .map_err(|e| format!("Failed to parse lowest version: {}", e))?;
            let highest = input[dash_index + 1..]
                .parse::<i16>()
                .map_err(|e| format!("Failed to parse highest version: {}", e))?;
            return Versions::new(lowest, highest);
        }

        let version = input.parse::<i16>().map_err(|e| format!("Failed to parse version: {}", e))?;
        Versions::new(version, version)
    }

    /// Creates a new version range.
    ///
    /// # Panics
    /// Returns an error if lowest or highest is negative.
    pub fn new(lowest: i16, highest: i16) -> Result<Self, String> {
        if lowest < 0 || highest < 0 {
            return Err(format!("Invalid version range {} to {}", lowest, highest));
        }
        Ok(Versions { lowest, highest })
    }

    /// Returns the lowest version in the range.
    pub fn lowest(&self) -> i16 {
        self.lowest
    }

    /// Returns the highest version in the range.
    pub fn highest(&self) -> i16 {
        self.highest
    }

    /// Returns true if this range contains no versions.
    pub fn empty(&self) -> bool {
        self.lowest > self.highest
    }

    /// Returns the intersection of two version ranges.
    pub fn intersect(&self, other: Versions) -> Versions {
        let new_lowest = self.lowest.max(other.lowest);
        let new_highest = self.highest.min(other.highest);
        if new_lowest > new_highest {
            return Versions::NONE;
        }
        Versions { lowest: new_lowest, highest: new_highest }
    }

    /// Returns a new version range that trims some versions from this range, if possible.
    /// We can't trim any versions if the resulting range would be disjoint.
    ///
    /// Some examples:
    /// 1-4.subtract(1-2) = Some(3-4)
    /// 3+.subtract(4+) = Some(3)
    /// 4+.subtract(3+) = Some(none)
    /// 1-5.subtract(2-4) = None
    pub fn subtract(&self, other: Versions) -> Option<Versions> {
        if other.lowest() <= self.lowest {
            if other.highest >= self.highest {
                // Case 1: other is a superset of this. Trim everything.
                Some(Versions::NONE)
            } else if other.highest < self.lowest {
                // Case 2: other is a disjoint version range that is lower than this. Trim nothing.
                Some(*self)
            } else {
                // Case 3: trim some values from the beginning of this range.
                //
                // Note: it is safe to assume that other.highest() + 1 will not overflow.
                // The reason is because if other.highest() were i16::MAX,
                // other.highest() < highest could not be true.
                Some(Versions { lowest: other.highest() + 1, highest: self.highest })
            }
        } else if other.highest >= self.highest {
            let new_highest = other.lowest - 1;
            if new_highest < 0 {
                // Case 4: other was NONE. Trim nothing.
                Some(*self)
            } else if new_highest < self.highest {
                // Case 5: trim some values from the end of this range.
                Some(Versions { lowest: self.lowest, highest: new_highest })
            } else {
                // Case 6: other is a disjoint range that is higher than this. Trim nothing.
                Some(*self)
            }
        } else {
            // Case 7: the difference between this and other would be two ranges, not one.
            None
        }
    }

    /// Returns true if this range contains the given version.
    pub fn contains(&self, version: i16) -> bool {
        version >= self.lowest && version <= self.highest
    }

    /// Returns true if this range contains all versions in the other range.
    pub fn contains_range(&self, other: Versions) -> bool {
        if other.empty() {
            return true;
        }
        self.lowest <= other.lowest && self.highest >= other.highest
    }
}

impl fmt::Display for Versions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.empty() {
            write!(f, "{}", Self::NONE_STRING)
        } else if self.lowest == self.highest {
            write!(f, "{}", self.lowest)
        } else if self.highest == i16::MAX {
            write!(f, "{}+", self.lowest)
        } else {
            write!(f, "{}-{}", self.lowest, self.highest)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_versions_parse() {
        assert_eq!(Versions::parse(Some(" none "), Versions::NONE).unwrap(), Versions::NONE);
        assert_eq!(Versions::parse(None, Versions::NONE).unwrap(), Versions::NONE);
        assert_eq!(Versions::parse(Some(" "), Versions::ALL).unwrap(), Versions::ALL);
        assert_eq!(Versions::parse(Some(""), Versions::ALL).unwrap(), Versions::ALL);
        assert_eq!(
            Versions::parse(Some(" 4-5 "), Versions::NONE).unwrap(),
            Versions::new(4, 5).unwrap()
        );
    }

    #[test]
    fn test_parse_single_version() {
        let v = Versions::parse(Some("5"), Versions::NONE).unwrap();
        assert_eq!(v.lowest(), 5);
        assert_eq!(v.highest(), 5);
    }

    #[test]
    fn test_parse_range() {
        let v = Versions::parse(Some("3-7"), Versions::NONE).unwrap();
        assert_eq!(v.lowest(), 3);
        assert_eq!(v.highest(), 7);
    }

    #[test]
    fn test_parse_open_ended() {
        let v = Versions::parse(Some("4+"), Versions::NONE).unwrap();
        assert_eq!(v.lowest(), 4);
        assert_eq!(v.highest(), i16::MAX);
    }

    #[test]
    fn test_parse_none() {
        let v = Versions::parse(Some("none"), Versions::ALL).unwrap();
        assert!(v.empty());
    }

    #[test]
    fn test_parse_default() {
        let v = Versions::parse(None, Versions::ALL).unwrap();
        assert_eq!(v, Versions::ALL);
    }

    #[test]
    fn test_round_trips() {
        test_round_trip(Versions::ALL, "0+");
        test_round_trip(Versions::new(1, 3).unwrap(), "1-3");
        test_round_trip(Versions::new(2, 2).unwrap(), "2");
        test_round_trip(Versions::new(3, i16::MAX).unwrap(), "3+");
        test_round_trip(Versions::NONE, "none");
    }

    fn test_round_trip(versions: Versions, string: &str) {
        assert_eq!(string, versions.to_string());
        assert_eq!(versions, Versions::parse(Some(&versions.to_string()), Versions::NONE).unwrap());
    }

    #[test]
    fn test_intersections() {
        assert_eq!(
            Versions::new(2, 3).unwrap(),
            Versions::new(1, 3).unwrap().intersect(Versions::new(2, 4).unwrap())
        );
        assert_eq!(
            Versions::new(3, 3).unwrap(),
            Versions::new(0, i16::MAX).unwrap().intersect(Versions::new(3, 3).unwrap())
        );
        assert_eq!(
            Versions::NONE,
            Versions::new(9, i16::MAX).unwrap().intersect(Versions::new(2, 8).unwrap())
        );
        assert_eq!(Versions::NONE, Versions::NONE.intersect(Versions::NONE));
    }

    #[test]
    fn test_contains() {
        let v = Versions::new(3, 7).unwrap();
        assert!(!v.contains(2));
        assert!(v.contains(3));
        assert!(v.contains(5));
        assert!(v.contains(7));
        assert!(!v.contains(8));

        // Test contains_range
        assert!(Versions::new(2, 3).unwrap().contains(3));
        assert!(Versions::new(2, 3).unwrap().contains(2));
        assert!(!Versions::new(0, 1).unwrap().contains(2));
        assert!(Versions::new(0, i16::MAX).unwrap().contains(100));
        assert!(!Versions::new(2, i16::MAX).unwrap().contains(0));
        assert!(Versions::new(2, 3).unwrap().contains_range(Versions::new(2, 3).unwrap()));
        assert!(Versions::new(2, 3).unwrap().contains_range(Versions::new(2, 2).unwrap()));
        assert!(!Versions::new(2, 3).unwrap().contains_range(Versions::new(2, 4).unwrap()));
        assert!(Versions::new(2, 3).unwrap().contains_range(Versions::NONE));
        assert!(Versions::ALL.contains_range(Versions::new(1, 2).unwrap()));
    }

    #[test]
    fn test_intersect() {
        let v1 = Versions::new(3, 7).unwrap();
        let v2 = Versions::new(5, 10).unwrap();
        let result = v1.intersect(v2);
        assert_eq!(result.lowest(), 5);
        assert_eq!(result.highest(), 7);
    }

    #[test]
    fn test_intersect_disjoint() {
        let v1 = Versions::new(1, 3).unwrap();
        let v2 = Versions::new(5, 7).unwrap();
        let result = v1.intersect(v2);
        assert!(result.empty());
    }

    #[test]
    fn test_subtract() {
        assert_eq!(Versions::NONE, Versions::NONE.subtract(Versions::NONE).unwrap());
        assert_eq!(
            Versions::new(0, 0).unwrap(),
            Versions::new(0, 0).unwrap().subtract(Versions::NONE).unwrap()
        );
        assert_eq!(
            Versions::new(1, 1).unwrap(),
            Versions::new(1, 2).unwrap().subtract(Versions::new(2, 2).unwrap()).unwrap()
        );
        assert_eq!(
            Versions::new(2, 2).unwrap(),
            Versions::new(1, 2).unwrap().subtract(Versions::new(1, 1).unwrap()).unwrap()
        );
        assert!(
            Versions::new(0, i16::MAX)
                .unwrap()
                .subtract(Versions::new(1, 100).unwrap())
                .is_none()
        );
        assert_eq!(
            Versions::new(10, 10).unwrap(),
            Versions::new(1, 10).unwrap().subtract(Versions::new(1, 9).unwrap()).unwrap()
        );
        assert_eq!(
            Versions::new(1, 1).unwrap(),
            Versions::new(1, 10).unwrap().subtract(Versions::new(2, 10).unwrap()).unwrap()
        );
        assert_eq!(
            Versions::new(2, 4).unwrap(),
            Versions::new(2, i16::MAX)
                .unwrap()
                .subtract(Versions::new(5, i16::MAX).unwrap())
                .unwrap()
        );
        assert_eq!(
            Versions::new(5, i16::MAX).unwrap(),
            Versions::new(0, i16::MAX)
                .unwrap()
                .subtract(Versions::new(0, 4).unwrap())
                .unwrap()
        );
    }

    #[test]
    fn test_to_string() {
        assert_eq!(Versions::NONE.to_string(), "none");
        assert_eq!(Versions::new(5, 5).unwrap().to_string(), "5");
        assert_eq!(Versions::new(3, 7).unwrap().to_string(), "3-7");
        assert_eq!(Versions::new(4, i16::MAX).unwrap().to_string(), "4+");
    }
}
