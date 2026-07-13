# Clojure Data Infrastructure Kit
# My Clojure Data Engineering Journey

## Why I Build with Clojure
I come from a non-IT background in biotechnology and e-commerce operations. I chose to learn Clojure over Python because I value speed, simplicity, and clear structure. I am actively looking for a remote data role to achieve long-term financial stability, and I am putting in 60+ hours a week of pure coding to make that happen.

## What My Code Can Do Right Now
Instead of just studying theory, I built a data pipeline from scratch. Here is what it actually achieves:

- **Handles Massive Files Easily**: It reads multi-gigabyte text files line-by-line without overloading my computer's RAM .
- **Cleans Messy Text Data**: It uses strict patterns to clear out bad spacing, spaces, and layout variations automatically .
- **Catches Bad Data**: If a row of data is broken or incomplete, my code separates it and saves it into a special "Error Log" file (Dead Letter Queue) so the main program never crashes.
- **Cleans Up After Itself**: It has built-in automated tests that verify the code works and automatically wipe temporary test files from my Windows drive.

## Tech Layout
- **Runtime**: Babashka / Clojure
- **Tools**: VS Code + Calva, Cheshire (for JSON files), clojure.test
