Technical Steering Committee
=============================

The Technical Steering Committee (TSC) is responsible for technical oversight of the project.

TSC Chair: **Viktor Somogyi-Vass** - @viktorsomogyi

TSC members:

  - **Adem Efe Gencer** - @efeg
  - **Hao Geng** - @CCisGG
  - **Nick Garvey** - @nickgarvey
  - **Maryan Hratson** - @mhratson
  - **Allen Wang** - @allenxwang
  - **Tamas Barnabas Egyed** - @egyedt / @egytom
  - **Chia-Ping Tsai** - @chia7712
  - **Krit Petty** - @bgrishinko
  - **Jiangjie (Becket) Qin** - @becketqin
  - **Viktor Somogyi-Vass** - @viktorsomogyi
  - **Omkhar Arasaratnam** - @omkhar
  - **Kondrat Bertalan** - @k0b3rIT
  - **Paolo Patierno** - @ppatierno
  - **Mickael Maison** - @mimaison
  - **Kyle Liberti** - @kyguy

At this time, all TSC members are voting members.

Maintainers
============

  - **Viktor Somogyi-Vass** - @viktorsomogyi
  - **Maryan Hratson** - @mhratson
  - **Krit Petty** - @bgrishinko

Per Section 2.c of the Charter, maintainers are contributors who can commit to the repo, and
are distinct from TSC voting members (though there can be overlap).

Contribution Agreement
======================

As a contributor, you represent that the code you submit is your
original work or that of your employer (in which case you represent you
have the right to bind your employer). By submitting code, you (and, if
applicable, your employer) are licensing the submitted code to
the open source community subject to the Apache 2.0 license. 


File Headers
=============

New files should include the following header:

```
/*
 * Copyright Cruise Control for Kafka contributors
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
```

Existing files with the original LinkedIn/BSD header should retain that header. (See [Charter, Section 7.a](CHARTER.md)


Responsible Disclosure of Security Vulnerabilities
==================================================

Please do not file reports on Github for security issues.
See [SECURITY.md](./SECURITY.md) for how to report a vulnerability.

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
(see [PR template](./.github/pull_request_template.md)), and PRs can be submitted directly when
repository's PR template is filled out with the details.
7. We strongly encourage the use of recommended code-style for the project 
(see [code-style.xml](./docs/code-style.xml)).
8. A pre-commit CheckStyle hook can be run by adding `./checkstyle/checkstyle-pre-commit` to your `.git/hooks/pre-commit` script.
