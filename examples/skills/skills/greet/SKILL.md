---
name: greet
description: Compose a friendly greeting and run the bundled greeter script
allowed-tools: [use_skill, run_skill_script]
---
# Greeting workflow

When asked to greet someone, follow these steps:

1. Call `use_skill` with `resource: "template.md"` to load the greeting
   template and see how the message should be phrased.
2. Run the bundled greeter with `run_skill_script` (skill `greet`, script
   `scripts/greet.sh`, `scripts/greet.py`, or `scripts/greet.js`); it prints
   the rendered greeting.
3. Report the greeting back to the user.
