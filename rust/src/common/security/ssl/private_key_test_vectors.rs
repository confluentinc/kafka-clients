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

//! Throwaway PEM test keys (never used anywhere else), for the tests of the private-key loading.
//!
//! Generated with OpenSSL 3, and checked to load in Java with the logic of
//! `DefaultSslEngineFactory.PemStore.privateKey`:
//!
//! ```text
//! openssl ecparam -name prime256v1 -genkey -noout | openssl pkcs8 -topk8 -nocrypt -out ec-plain.pem
//! openssl req -new -x509 -key ec-plain.pem -subj "/CN=kafka-client" -days 36500 \
//!     -addext "basicConstraints=critical,CA:FALSE" -addext "extendedKeyUsage=clientAuth" -out ec-cert.pem
//! openssl pkcs8 -topk8 -v2 <cipher> -v2prf <prf> -iter 2048 -passout pass:<PASSWORD> \
//!     -in ec-plain.pem -out ec-enc-<cipher>-<prf>.pem
//! ```
//!
//! The RSA files are made the same way from `openssl genpkey -algorithm RSA`.

/// The password every encrypted key below is encrypted with (non-ASCII on purpose: PBKDF2 takes UTF-8).
pub(crate) const PASSWORD: &str = "S3cr3t-pass_üñí";

/// Self-signed certificate of the EC key.
pub(crate) const EC_CERT: &str = r#"-----BEGIN CERTIFICATE-----
MIIBmDCCAT2gAwIBAgIUQYdjDGt6tiqaJRADarT+wAnQ+OgwCgYIKoZIzj0EAwIw
FzEVMBMGA1UEAwwMa2Fma2EtY2xpZW50MCAXDTI2MTAxNzEzMTE0MFoYDzIxMjYw
OTIzMTMxMTQwWjAXMRUwEwYDVQQDDAxrYWZrYS1jbGllbnQwWTATBgcqhkjOPQIB
BggqhkjOPQMBBwNCAARYlZWiFIdjT78c9kozxypieDtQKQRhhArt/psQ03Kljx2I
hNbB+TbMaOHe1SK64qEwCTWLUREj0g2EBqNAIUObo2UwYzAdBgNVHQ4EFgQUQrCP
5l9EdGIgr+V/4LJEIPQy7TMwHwYDVR0jBBgwFoAUQrCP5l9EdGIgr+V/4LJEIPQy
7TMwDAYDVR0TAQH/BAIwADATBgNVHSUEDDAKBggrBgEFBQcDAjAKBggqhkjOPQQD
AgNJADBGAiEAxlnO6/rS84ULUGzq9FNbd0HEGoBKDdm3jjw492opMrsCIQCwX44E
W8eaJ7NmzbYYqNOtovSkbRSjJUeJIymrXr/zTw==
-----END CERTIFICATE-----
"#;

/// The EC key, unencrypted PKCS#8 (`PRIVATE KEY`).
pub(crate) const EC_PLAIN: &str = r#"-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgGDX154rJXJ0fHGUr
ma/JKzKZlXwls3NVZouGd/DYGpqhRANCAARYlZWiFIdjT78c9kozxypieDtQKQRh
hArt/psQ03Kljx2IhNbB+TbMaOHe1SK64qEwCTWLUREj0g2EBqNAIUOb
-----END PRIVATE KEY-----
"#;

/// The EC key as `ENCRYPTED PRIVATE KEY`: PBES2, aes-256-cbc-hmacWithSHA256.
pub(crate) const EC_ENC_AES256_SHA256: &str = r#"-----BEGIN ENCRYPTED PRIVATE KEY-----
MIH0MF8GCSqGSIb3DQEFDTBSMDEGCSqGSIb3DQEFDDAkBBCp+kKJ8R/mOjWw3nCq
nMIgAgIIADAMBggqhkiG9w0CCQUAMB0GCWCGSAFlAwQBKgQQkMY1CjCxncLY/pj0
SfDhowSBkBKbWudRWRRxMU0HQafNwUicmBY+cFRswe1QNk2FaN20/KnwZOsV0BU4
oiPt0t7r2bEI5SGYCNvcnzqUOU++OsY3VhFGufu06riqf1wBCtiBMALjJC8P2fmk
/N4nkLKK3yIJjEKbbL0nGXfIYFzFf1MnemZWP/vupFtS2fB2tsfv0Ngs67ECsfET
V8Srtsolfg==
-----END ENCRYPTED PRIVATE KEY-----
"#;

/// The EC key as `ENCRYPTED PRIVATE KEY`: PBES2, aes-128-cbc-hmacWithSHA1.
pub(crate) const EC_ENC_AES128_SHA1: &str = r#"-----BEGIN ENCRYPTED PRIVATE KEY-----
MIHmMFEGCSqGSIb3DQEFDTBEMCMGCSqGSIb3DQEFDDAWBBBIH3/69zZF6WgeuEnY
p55XAgIIADAdBglghkgBZQMEAQIEELXUaLJ5PoI/LZew8KNT0wwEgZDvROQvA+9M
z5xj/mtvWTqjrtmThZsDF7q2T8e8cg+xT7w99tAWmVOD0UjSE/AJoLTU8WL3mKcA
9oB02ClUaVM0EHSp9z5grhjQftoJ4zXHgpx4ZDvONCS8eRqnwGOJ2wQ/Rg221NVU
CRJNl5TII6EsB8ooQmOjHISz2/0hkJ+Q8E7Fdvyr2UvZ4VCxwx1s1qs=
-----END ENCRYPTED PRIVATE KEY-----
"#;

/// The EC key as `ENCRYPTED PRIVATE KEY`: PBES2, aes-128-cbc-hmacWithSHA256.
pub(crate) const EC_ENC_AES128_SHA256: &str = r#"-----BEGIN ENCRYPTED PRIVATE KEY-----
MIH0MF8GCSqGSIb3DQEFDTBSMDEGCSqGSIb3DQEFDDAkBBD2xVpTyj2J2R2D6+rP
TI1XAgIIADAMBggqhkiG9w0CCQUAMB0GCWCGSAFlAwQBAgQQKawOEByoy1UWFC18
zi7htgSBkCieC4jibUT0QQsBJrprVHwyKbkCKSJfg97OELvtrDE7s5KJI8q7k0co
QbJ9LBH93DVOKDALbkufVzsAqJoKF1RGu332g1O0b3YfemJlY3FslFamJn7/ppZ8
tuBOrxWn0fRw830kDjLO5SM94mtOnIJyVRGAcJ7oW1mPHEDHm3GrguTb/m0SY/DP
svEypkWINQ==
-----END ENCRYPTED PRIVATE KEY-----
"#;

/// The EC key as `ENCRYPTED PRIVATE KEY`: PBES2, aes-256-cbc-hmacWithSHA224.
pub(crate) const EC_ENC_AES256_SHA224: &str = r#"-----BEGIN ENCRYPTED PRIVATE KEY-----
MIH0MF8GCSqGSIb3DQEFDTBSMDEGCSqGSIb3DQEFDDAkBBCC08QWZ32ju/j4MiZo
Y12BAgIIADAMBggqhkiG9w0CCAUAMB0GCWCGSAFlAwQBKgQQrdmHK1O9Cv5z3t44
8NbZ2wSBkOX+xO3NldF6KlDybn9ZBi4UZzc4CgTD0voqm9990chvfPm3q/0bhCr4
YfDbm31E3M4tsTL733S5pGg+nuUDRHzoIV4/7gaVZynfQWh71Hkwzd69UInLNCmE
cfp3rQ+wCjaxF8eDIvnoi9CM1jz2qlpHoR2nZvv2wGRJ+CO2VWaBi/8XY7saxqeS
4kfQPtcuBg==
-----END ENCRYPTED PRIVATE KEY-----
"#;

/// The EC key as `ENCRYPTED PRIVATE KEY`: PBES2, aes-256-cbc-hmacWithSHA384.
pub(crate) const EC_ENC_AES256_SHA384: &str = r#"-----BEGIN ENCRYPTED PRIVATE KEY-----
MIH0MF8GCSqGSIb3DQEFDTBSMDEGCSqGSIb3DQEFDDAkBBBvS/v1ix58yUrsivy1
jCHeAgIIADAMBggqhkiG9w0CCgUAMB0GCWCGSAFlAwQBKgQQzKAeWK8sLoFcCoPC
bsv70ASBkGCEDzw0N7AT4ld0fbQRFeVwaratnq7mfx01SxkAw62bhnwqVwS2H+gl
+IwQC280vD/DGhqpmPAcm43sC48orsZXXwpx0zwn8mEcf6I1zkd+aMkHuMfydgcQ
krOeo95+w4a13IrwZWfcHZMVqKb+6nHJG2RaLX3U9dlELFE9o/otP1Xh9TvBT0d7
YgvZxcEJPg==
-----END ENCRYPTED PRIVATE KEY-----
"#;

/// The EC key as `ENCRYPTED PRIVATE KEY`: PBES2, aes-256-cbc-hmacWithSHA512.
pub(crate) const EC_ENC_AES256_SHA512: &str = r#"-----BEGIN ENCRYPTED PRIVATE KEY-----
MIH0MF8GCSqGSIb3DQEFDTBSMDEGCSqGSIb3DQEFDDAkBBBYw+UDwGOu98CiAqI1
qO3gAgIIADAMBggqhkiG9w0CCwUAMB0GCWCGSAFlAwQBKgQQLiyU3xIxclrtYmlV
1lYzVwSBkJVlQ+/0L+0FKSs/acn4KnxPDigCq7Hk9R8SnQgTprCgunRIX1C4I05a
T/HxwMI+/UxFx4LHV2sjOvNHQleppMmFYb0dLsknjEvSyyBfSo7fEePmCVMZ3HD/
oz7mRM21UxpWU8z03ckrr+o/tY+RxY7hW+GeGsZDso8N3/nHsT+CH6B3sqWgw0yf
2QTvga5wTw==
-----END ENCRYPTED PRIVATE KEY-----
"#;

/// AES-192-CBC, which the JDK cannot read either (`expecting the object identifier for AES cipher`).
pub(crate) const EC_ENC_AES192_SHA512: &str = r#"-----BEGIN ENCRYPTED PRIVATE KEY-----
MIH0MF8GCSqGSIb3DQEFDTBSMDEGCSqGSIb3DQEFDDAkBBAsfqlNnCXcI3v11d5q
uFCiAgIIADAMBggqhkiG9w0CCwUAMB0GCWCGSAFlAwQBFgQQ6qohx+DKRcqhOziB
DzJGSwSBkDLVaYEY6OD3GqpIyy8uKm6qC4+i56OMx4wYprZVJH33OZwbDh2Ygh6b
ZXaCkNVsGI6Hk++dHzogPYS9MJPkr2T8n0gPXJimdIlJXctqh8YDeh0SuSAMGjZN
E7p1F3C3BueetLmORvg7muYQUbnf7IdyBZKtbMAxtI4DHjQPNORmiMk1M7UX2LOV
I/lu/l+Ylg==
-----END ENCRYPTED PRIVATE KEY-----
"#;

/// PBES2 with scrypt (`openssl pkcs8 -topk8 -v2 aes-256-cbc -scrypt`), which the JDK cannot read (`expecting the object identifier for PBKDF2`).
pub(crate) const EC_ENC_SCRYPT: &str = r#"-----BEGIN ENCRYPTED PRIVATE KEY-----
MIHsMFcGCSqGSIb3DQEFDTBKMCkGCSsGAQQB2kcECzAcBBAGKUahkLYlRAvv/gdY
8HtWAgJAAAIBCAIBATAdBglghkgBZQMEASoEEJHrAiePokTk5Jh9uzEbY2sEgZC8
ebBGoPHU5q+gYLfMdhrZKiH5R4s/dwdcwT0hYhEaK6iwIAXAE4R0sHd+Q1mWz4VI
maqK7dqjK4wzhHqyFT8gVCg1s72tZN5wrD399uKgnMFumN4zF/5nrvR82X6vfa2g
RzyWz3fPf/6J+1YC2LggL+wAJLZB6AJDVxP3e8coEtPPhE1e0H5I4icNX8FE8f4=
-----END ENCRYPTED PRIVATE KEY-----
"#;

/// PBES2 with Camellia-256-CBC, which the JDK cannot read (`expecting the object identifier for AES cipher`).
pub(crate) const EC_ENC_CAMELLIA256: &str = r#"-----BEGIN ENCRYPTED PRIVATE KEY-----
MIH2MGEGCSqGSIb3DQEFDTBUMDEGCSqGSIb3DQEFDDAkBBCNS09LRM/m7GhxTp+1
Ea0DAgIIADAMBggqhkiG9w0CCQUAMB8GCyqDCIyaSz0BAQEEBBCoXIFixHFPt/tt
tikG0u1RBIGQ1b2H6ozd2q9/f0P71x8UjUu33NmmSOUb8kmtG5HmKKh7Ykprv9G9
RWvDzJgzVt5nCA3VhXdMnuwd97CTf+DjCyMV2KDRSxlE0NJfkHs8jS57SADIyKtw
OK1hFw3NdpefgtQyS5ylPszy0pbTBacVwtZYVoHPd2Zsdu+/HvvM0846N1KFL1c3
HENX5CLi/xcg
-----END ENCRYPTED PRIVATE KEY-----
"#;

/// PBES2, aes-256-cbc with hmacWithSHA512-224: the JDK reads it, this client does not.
pub(crate) const EC_ENC_AES256_SHA512_224: &str = r#"-----BEGIN ENCRYPTED PRIVATE KEY-----
MIH0MF8GCSqGSIb3DQEFDTBSMDEGCSqGSIb3DQEFDDAkBBBVLi2lBtQZZqgaSigH
SnuNAgIIADAMBggqhkiG9w0CDAUAMB0GCWCGSAFlAwQBKgQQN1XyLQ+InNBAFqYW
yTD2+ASBkAvpXXtNiWfkHYJRrsxWg1VxUnRYUkobTHQB7cQKPn/vYN+7UPkuChdi
IsaubO6WJuml20oaY7qEmx3Q7yHdVylnN9fYOSGMZTL1g+XD/y9rdH5aFuGX1B+a
zrexFwmI+SIsgjcAsVCXcQwIX+5gJxXLo/viNWdKifsa7n11l+2U0ujelELSgXkC
S5SyX1Fzjg==
-----END ENCRYPTED PRIVATE KEY-----
"#;

/// PBES2, aes-256-cbc with hmacWithSHA512-256: the JDK reads it, this client does not.
pub(crate) const EC_ENC_AES256_SHA512_256: &str = r#"-----BEGIN ENCRYPTED PRIVATE KEY-----
MIH0MF8GCSqGSIb3DQEFDTBSMDEGCSqGSIb3DQEFDDAkBBBjCBgDW8MLPVD5ZxSS
vjp1AgIIADAMBggqhkiG9w0CDQUAMB0GCWCGSAFlAwQBKgQQMcseqWR6IUCCbvQQ
mHzPxASBkGe8GDsMap2zQx3inAc5gA7SPgU/1PSTc8n+b+3jaakesyz4dWk/aY8b
0fJe683M9uURuD7lQiBfC30u7DYqaXlKKB3AzGsHgO9i8CGxF6y7v1BHbU8I82KO
xysYBQQ1YFm8oMbTOjUwQrbjNf30qO7XVF2qoeN5XTgx9vsBp1OVJ2miIReEBRDb
p37RyEFdPg==
-----END ENCRYPTED PRIVATE KEY-----
"#;

/// Self-signed certificate of the RSA key.
pub(crate) const RSA_CERT: &str = r#"-----BEGIN CERTIFICATE-----
MIIDKzCCAhOgAwIBAgIUTkD/3qGG8LGAqFZnHLhn8NjhGocwDQYJKoZIhvcNAQEL
BQAwGzEZMBcGA1UEAwwQa2Fma2EtY2xpZW50LXJzYTAgFw0yNjEwMTcxMzExNDBa
GA8yMTI2MDkyMzEzMTE0MFowGzEZMBcGA1UEAwwQa2Fma2EtY2xpZW50LXJzYTCC
ASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAKlRLgMFwVD8g4yRtpIH6CaR
ahRsPyrkMj/tL/E+cjaK3Yh/ejRTletNw4UrEIqicb+FLzQ2akteyb3GXsx8FGMT
0twAEeEp2ky00JdbW0HuO8k7Zq3d7fpFkODRJ1lpHkTlVjptBcV6rRx+GVQ8NceH
d/zcnWsQ3YZqXR2ZzzbMDUyslgM1YnkBwwx5Zh+fA5j37JLzB6kC6FtG353CjIHN
Lh7FF9Gzc7uH9kAnHv8M1OTx5d8RtsbP8iXXiuMm2gPbGERHPoGSm6N7FSoCIv0R
0Uu84NDNVwj2Iu3Op/AxVJ302K4AupqBn6Cfy3oxFblisKoeZZ/G3IitcOXApvMC
AwEAAaNlMGMwHQYDVR0OBBYEFNX7skOyL+EXBeCrMC1suNZTFuEnMB8GA1UdIwQY
MBaAFNX7skOyL+EXBeCrMC1suNZTFuEnMAwGA1UdEwEB/wQCMAAwEwYDVR0lBAww
CgYIKwYBBQUHAwIwDQYJKoZIhvcNAQELBQADggEBADAMcpYA0GmAeGsM4A3DB7IT
NXuRl6u7z1LYKdqLdi8pIhNPQ7mo767Taux2VzoR3HMXBmDAKwBDdt1OwyUI4EeK
Na/AyJler8O5q/dEu3I9HFFzvGgVbJAh/QN2FnA4lHYNoCh08d0robSBDLelVCZK
RNv4v1PdAiQQ/UyFL9m8511F+i2D7aflHDAKZySBiH1mWdC7kEQZF7iR2b7gB+Wx
gMucj8OO44plFG88GipHxRPMHD7uXzHT9FI4AQoSTfqwKan9l0UV/QypitTltW2B
czMTCm76gACfQiE1W8rDmt1MQ6KarcD9tvloaazcOXs4DMCdugCDjdKZYJdj/PY=
-----END CERTIFICATE-----
"#;

/// The RSA key, unencrypted PKCS#8.
pub(crate) const RSA_PLAIN: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCpUS4DBcFQ/IOM
kbaSB+gmkWoUbD8q5DI/7S/xPnI2it2If3o0U5XrTcOFKxCKonG/hS80NmpLXsm9
xl7MfBRjE9LcABHhKdpMtNCXW1tB7jvJO2at3e36RZDg0SdZaR5E5VY6bQXFeq0c
fhlUPDXHh3f83J1rEN2Gal0dmc82zA1MrJYDNWJ5AcMMeWYfnwOY9+yS8wepAuhb
Rt+dwoyBzS4exRfRs3O7h/ZAJx7/DNTk8eXfEbbGz/Il14rjJtoD2xhERz6Bkpuj
exUqAiL9EdFLvODQzVcI9iLtzqfwMVSd9NiuALqagZ+gn8t6MRW5YrCqHmWfxtyI
rXDlwKbzAgMBAAECggEAAk/UR9XzcpvNX1p6pS92ySwGyETwGdxSyVJU62orB2P0
VrxIMAsMytEy1bslodb0+aSjuoacWJobOWb00r2bo6dYzjMxYKMrKA1MpHxqrbxH
KbNmGVf55gcUItWy6q1yQduQcVdyvY8JamWttTDl78BW8DzCPid/9tQLSxpXXMv9
D1a14x+Y8SpuoDtZPE3tCVXo4rBvX5+45hxvdZO57foxJjDsRBtNwcRfaN66kCf0
bt/bmuLfIKMViwQYU1qO0sH6GKxUxtlf1bsoB2wbxjl0CcOj1nhM/L5PiM8mCio9
TuCio0dB7wnXzN+anbToE8NPzcjib6YUh5kLB505+QKBgQDrco6rZQDW/F1Ga+wa
nzCc0ExM8DmxqAZ8IDJ6MmJiWhkvON131jyqrgmnDtDEzVYQXdnOBOifvPlKsv4+
Eu4s/uKR/Wp4qVLAY83xAtaEdMLxywYI0uNdCS+KxccLjuUYgB8beawuIiMUBuhM
1knyRh26Z4IiAbsE/CAB+bLeyQKBgQC4GNXmqQiENjSvvfp+egsgkfKnJBYnNjfs
HJBAalI9aF8igGswfs45Uk7Q6UyYyOfajrw3rlkmDjZYppdOl5wKViIxH3+eQ/cv
eJwwDQB1kxENBhM4d459HA+P9/Zkvej4bG37BhwImEjA5MtVQm+p6QafpMeXPZUp
CGhA0S8J2wKBgQCz/gatZnCm6ZSRAm4jx/DM4LyD0N36P559g8uy+ROXZpN8Mzez
pjcIsDW23a+h0+voZSbdxYjknP3jKkUMxUIJae1hd97OUXtukp3ckrPV3MEvQWri
kf24oQ4UQ7Av2NSwzzl4sqJ7PiOc6s7p9qPfL9UILJsiTjpfzDLX78DbaQKBgC2a
4WpJl3WBJB4hFuu7+gOwhAcJtn8yPDo+fTGn5QWXVHHslCEoWJf0nte746C5rE4F
K/UzOJxnyui9JCBxCFO7MK8wmWGwfoyA+Vgh8Gov63ppddt4KpEtoWFN0Pj13r+j
xL75rXonl2ZJh2Fwms7hjiWAIX2C4O+QSJs99sQfAoGAXLs60uagpP+pkei9DwcP
ZGHnSXnJ8ANBcKmUVMqM2m6VPDKW0jrtkpyD3CecVgm14oUvgn5VLQdDTqdck6SL
0pzrei7kIcnb1Cf7L2t2LqfdZ37ssDrzAYsfFbN/Gg9mjBdOH2Rs6G7rcLfautSO
J6iKCaGlgQGafGdPXnOSxm8=
-----END PRIVATE KEY-----
"#;

/// The RSA key as `ENCRYPTED PRIVATE KEY`: PBES2, aes-256-cbc-hmacWithSHA256.
pub(crate) const RSA_ENC_AES256_SHA256: &str = r#"-----BEGIN ENCRYPTED PRIVATE KEY-----
MIIFNTBfBgkqhkiG9w0BBQ0wUjAxBgkqhkiG9w0BBQwwJAQQs7vEJpJWJys3muzf
PajzMAICCAAwDAYIKoZIhvcNAgkFADAdBglghkgBZQMEASoEEFuaoqmHkq8B84W9
UW1qFakEggTQ+uBOqI0zdHxpbVf98rDuqOBSWjo7thBs6WepNb6QGudeyfSbv03P
IpWBEgH9YU4ZWOg4388MfHZWXbVHOqAZeIuJTc0sVIbTdBwFehtrq/Wx+/tsYbIg
IGN6CFX6uw7uF3wo2HvCy4aUOtQB6sofrDJ2Jv3Z7GVjJG65/Unv6BBU0v6cRK6B
gFFQ2EV+LxxEeO+CN8rNc32/9ymkK9VRU755IDjrh0KA2Ycx9OT45TN1k1rDoOT3
EPgkPNv67Vk2/xcLiZCdD+TQSRD17GuSLvJ8fMfMfQyuyzIes1PcO9WPA2qo131z
EE1KMyrOxg4HhROn3zRubVSjBvpYZMULE2Kn7Pgl4ZdxWbp8PYnsMgmYh/gvgGg4
ObqIaF0+vgVufQ6/QOcoyq4rH9KWcWIfEeO2PpflMVzHRU45aAnLNm37qChxkQZa
4EtHAZFjpJIm57iIeR/2ZKSUhjhALyn6xjRzq/fqHBGgCkzrK+KRTF1TieLPB31o
MgJVfrkY1s41bbPi/6Bg3t3urhIJAWE3asq6q36Ums8AWxGO5d2kqdtX33x5pdFE
40x/OKtyJVE2DqBjPUw+0OrvPZznZgPaY76iF6DxAzFAAyQNk95doGJ8yvzvPAem
1tdIVA5d26l8m+7cmJP3Cg8k8zrdno2oQgRbftC9xmizzC6qS/nIQrD1D5Z20lty
DYBklEUAZiCSxXNwEuKv7meoRX2+Chz/T+Tm2E2LZJ9RvhuwxemildmjLlY2uOVX
tDzWZv4tUNqHriL3Ho2BsECSRVAJWz5I/UFfkdCNUoL+54P8ATpjggBZPOeml9D8
oA6LbhFR8e/Pzj8rPaZjXoiEWpD1QEL2iVckQPC9YEef1WzzQ++h8CA2CJuMFYfU
y6KHhW1P2yua2yRzpDh/a85CFd/3ius1uBWIuMMZUX1L8tzXz/zfNC2nfseg3PMj
ttQ58OhtdwRQB5nXXfviZm9mSUeiu/HWV/jx2CDt+G+xTjJpSCzRBGbCPcXXeolv
ItuLe6DpTXJAIDOdbrxNAnRFWGWeNehfikc7iNU/xUnOnauPvZmGCgj84T28b+uJ
OO7nKS+ERmvIf3M6D8MHp/SkegxXjdtV8+6NeOJUb9UCCaqkXgnri8A2UmnXR2bO
qi45TZS80Kl/7Jp+N8QzwkTtSKMuTUXPA+qckwCmjMiSfQNtcfjDCRdEfUoTzOjX
eA6VmSHAjCKl9C4ekmBrPy12ey2SeQceEmxYyeD3Nb3VVM/mtEVpLeKD1lmMFoDm
bHayhEBF+wF6nJGlsm8zzpI2C3bwLsDZ11aB1UzlXhMPxOy7AonDWK8tEkNG7wcc
LWNrDFi5LWHdzfDth2nuV0JhBWRPJpt+ArbvFr0NC4zptztDLeCw3AmlvfV0PTcQ
3hql7gbrZUQpnKXOWGdL5ZVhkErt56/il9FncJkawhAQqqLSgYNHD7kdHiakA39z
chY57Pphi7trbm+hurCB4acbbzQeageuGFKCcYrAeV+CNHao/zzA+FaM9uVhdXnJ
zouwKauR5nyCyFHyzpgyD5j8mGhxBD3Pcp2qYa1A80Yb/bb6cmPBiguncn1Qk5BR
BCynj0DaC5UFw/fL/Wgkgo0IwuxBRl+2e4WY47xgmZ+fu1sqq8haRyM=
-----END ENCRYPTED PRIVATE KEY-----
"#;
