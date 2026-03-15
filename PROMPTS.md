# PROMPTS.md — Prompts Used to Build This Project

A log of the prompts I used with Claude Code to iteratively improve this project.

---

### README cleanup

> In the README, remove the clone the repo. What is the point of it.

> Go through readme.md file, look through old comments and remove what is no longer there e.g python main.py init

> Explainer video link is https://youtu.be/Zvvt4kD0nRI add to the readme.md at the very top

> Skim through the entire project and make sure both readme.md and solutions.md are updated with the latest info to date

---

### ETL logic changes

> When resolving duplicates, lets rather keep the latest record instead of oldest

> Invalid emails should be checked with regex, not just absence of @

> Database.py customers table schema doesn't have lowercasing email

---

### Feature implementation (from SOLUTION.md "What I would add with more time")

> Please open the SOLUTION.md file and navigate to the section at the end titled "What I would add with more time". Identify the first bullet point in that section. Then assign an appropriate agent to implement the work described in that first point.
>
> *(Quarantine table to persist rejected rows and load to the relevant views)*

> Please open the SOLUTION.md file... Identify the second bullet point... Then assign an appropriate agent to implement the work described in that second point.
>
> *(Unit tests for each transform function)*

> Please open the SOLUTION.md file... Identify the third bullet point... Then assign an appropriate agent to implement the work described in that third point.
>
> *(Exception handling for database.py)*

> Please open the SOLUTION.md file... Identify the fourth bullet point... Then assign an appropriate agent to implement the work described in that fourth point.
>
> *(Docker Compose setup so reviewers don't need to install PostgreSQL manually)*

---

### Debugging

> Python main.py failed to run, please check it out

> Please execute pytest tests/ it failed to run, there was an error

---

### Documentation and project setup

> Create a diagram of how the python scripts relate to one another and what they do. Make it brief.

> Add the diagram to the README at the top.

> Add a brief summary of the project at the top of the README.

> Add pycache to gitignore.

> Set up venv for this project.

> Add venv setup and how to activate your venv to the README.

> Update README setup steps — clarify database creation step and link it to `python main.py init`.

> Fix README to show Windows `copy` command alongside Mac/Linux `cp`.

> Change "analytics team" to "relevant team" in the ETL quarantine docstrings.

> Update SOLUTION.md "What I would add with more time" section to reflect what has been implemented.

> Document all my prompts into a PROMPTS.md file.