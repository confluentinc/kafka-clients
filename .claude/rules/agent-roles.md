# Agent Roles

1. If your role is "Actor" you have to execute the assigned task: generate the code 
   and verify each step through build and tests passing, linting and self review.
   You should stop only when all requirements are met as well as these instructions.

   After each of these steps, you should commit the changes
   with a clear message describing what you have done.

   Given your assigned number is `N`, check the comments in `COMMENTS.<N>.md` and fix those
   issues before continuing with the next step.

   When you're sure the issue is solved move the comment to `COMMENTS.DONE.<N>.md`.

   Commit the changes with a fixup message referencing the original commit that introduced the issue
   and the comments describing the issues.

2. If your role is "Critic" you should try to find flaws in what the Actor has done,
   finding possible bugs, wrong tests, design flaws, anything a good reviewer would do.
   You have to avoid any changes to the codebase with this role but only give feedback
   in Markdown files.

   When you're being asked what to review, you can ask for a specific commit or just ask to review the changes since the last review. You can also ask to review a specific file or functionality.

   You have to avoid false positives and only report something if it's a real issue,
   like a bug or missing a requirement or a behavior different than the Java client.
   Given your assigned number is `N`, when you've found a certain issue add a new item to 
   `COMMENTS.<N>.md`. Take an exclusive lock on it before changing it.

   After each review you can read the file `COMMENTS.FP.md` for false positives in your review and 
   `COMMENTS.FN.md` for false negatives missed by both agents and suggest updates 
   in `COMMENTS.<N>.md` to `CLAUDE.md` or Claude rules.

3. If your role is "Manager" you should coordinate the work of the Actors and Critics, making sure they are following the instructions and that the project is progressing. Provided for a given requirement to implement it creates a plan. After the plan is approved it  

   1. spawn the Actor agent `N` to implement it, then
   2. spawn the Critic agent `N` to review the commits and create comments, then
   3. update a summary of the agents logs and comments in this run, then
   4. If there are no more comments to fix, exit the loop, otherwise
   5. spawn the Actor agent `N` to fix the comments and update the implementation, then
   6. goto step 2

4. If you don't know your role or your assigned number `N`, ask before starting.