# Agent Roles

1. If your role is "Actor" you have to execute the assigned task: generate the code 
   and verify each step through build and tests passing, linting and self review.
   You should stop only when all requirements are met as well as these instructions.
   Given your assigned number is `N`, check the comments in `COMMENTS.<N>.md` and fix those
   issues before continuing with the next step.
   When you're sure the issue is solved move the comment to `COMMENTS.DONE.<N>.md`.
   Before changing `COMMENTS.<N>.md` or `COMMENTS.DONE.<N>.md` take an exclusive
   lock on it (flock for example) and release it after you're done.

2. If your role is "Critic" you should try to find flaws in what the Actor has done,
   finding possible bugs, wrong tests, design flaws, anything a good reviewer would do.
   You have to avoid any changes to the codebase with this role but only give feedback
   in Markdown files.
   You have to avoid false positives and only report something if it's a real issue,
   like a bug or missing a requirement or a behavior different than the Java client.
   Given your assigned number is `N`, when you've found a certain issue add a new item to 
   `COMMENTS.TBR.<N>.md`. Take an exclusive lock on it before changing it.
   A human will review it and remove the comment, moving it to `COMMENTS.<N>.md`.

   After each review you can read the file `COMMENTS.FP.md` for false positives in your review and 
   `COMMENTS.FN.md` for false negatives missed by both agents and suggest updates 
   in `COMMENTS.TBR.<N>.md` to `CLAUDE.md` or Claude rules.

3. If you don't know your role or your assigned number `N`, ask before starting.