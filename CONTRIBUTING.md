Contribution Agreement
======================

As a contributor, you represent that the code you submit is your
original work or that of your employer (in which case you represent you
have the right to bind your employer). By submitting code, you (and, if
applicable, your employer) are licensing the submitted code to LinkedIn
and the open source community subject to the BSD 2-Clause license. 

Responsible Disclosure of Security Vulnerabilities
==================================================

Please do not file reports on Github for security issues.
Please review the guidelines on at 
https://www.linkedin.com/help/linkedin/answer/62924/security-vulnerabilities?lang=en

Tips for Getting Your Pull Request (PR) Accepted
===========================================

1. Make sure all new features are tested and the tests pass -- i.e. a submitted PR should have already been tested for 
existing and new unit tests.
2. Bug fixes must include a test case demonstrating the error that it fixes.
3. Open an issue first and seek advice for your change before submitting a PR. Large features which have never been 
discussed are unlikely to be accepted.
4. Do not create a PR with "work-in-progress" (WIP) changes.
5. Use clear and concise titles for submitted PRs and issues.
6. Each PR should be linked to an existing issue corresponding to the PR 
(see [PR template](https://github.com/linkedin/cruise-control/blob/migrate_to_kafka_2_4/docs/pull_request_template.md)).
7. If there are no existing issues about a PR, create one before submitting the PR.
8. We strongly encourage the use of recommended code-style for the project 
(see [code-style.xml](https://github.com/linkedin/cruise-control/blob/migrate_to_kafka_2_4/docs/code-style.xml)).
9. A pre-commit CheckStyle hook can be run by adding `./checkstyle/checkstyle-pre-commit` to your `.git/hooks/pre-commit` script.
