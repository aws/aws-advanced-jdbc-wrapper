# Release Schedule

| Release Date      | Release                                                                                  |
|-------------------|------------------------------------------------------------------------------------------|
| October 5, 2022   | [Release 1.0.0](https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases/tag/1.0.0) |  
| January 31, 2023  | [Release 1.0.1](https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases/tag/1.0.1) | 
| Mar 30, 2023      | [Release 1.0.2](https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases/tag/1.0.2) |
| April 28, 2023    | [Release 2.0.0](https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases/tag/2.0.0) |  
| May 11, 2023      | [Release 2.1.0](https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases/tag/2.1.0) |
| May 21, 2023      | [Release 2.1.1](https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases/tag/2.1.1) |
| June 14, 2023     | [Release 2.2.0](https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases/tag/2.2.0) |
| June 16, 2023     | [Release 2.2.1](https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases/tag/2.2.1) |
| July 5, 2023      | [Release 2.2.2](https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases/tag/2.2.2) |
| July 31, 2023     | [Release 2.2.3](https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases/tag/2.2.3) |
| August 25, 2023   | [Release 2.2.4](https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases/tag/2.2.4) |
| October 3, 2023   | [Release 2.2.5](https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases/tag/2.2.5) |
| November 15, 2023 | [Release 2.3.0](https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases/tag/2.3.0) |
| November 29, 2023 | [Release 2.3.1](https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases/tag/2.3.1) |
| December 18, 2023 | [Release 2.3.2](https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases/tag/2.3.2) |
| January 23, 2024  | [Release 2.3.3](https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases/tag/2.3.3) |
| March 1, 2024     | [Release 2.3.4](https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases/tag/2.3.4) |
| March 14, 2024    | [Release 2.3.5](https://github.com/awslabs/aws-advanced-jdbc-wrapper/releases/tag/2.3.5) |

`aws-advanced-jdbc-wrapper` [follows semver](https://semver.org/#semantic-versioning-200) which means we will only
release breaking changes in major versions. Generally speaking patches will be released to fix existing problems without
adding new features. Minor version releases will include new features as well as fixes to existing features. We will do
our best to deprecate existing features before removing them completely.

For minor version releases, `aws-advanced-jdbc-wrapper` uses a “release-train” model. Approximately every four weeks we
release a new minor version which includes all the new features and fixes that are ready to go.
Having a set release schedule makes sure `aws-advanced-jdbc-wrapper` is released in a predictable way and prevents a
backlog of unreleased changes.

In contrast, `aws-advanced-jdbc-wrapper` releases new major versions only when there are a critical mass of
breaking changes (e.g. changes that are incompatible with existing APIs). This tends to happen if we need to
change the way the driver is currently working. Fortunately, the JDBC API is fairly mature and has not changed, however
in the event that the API changes we will release a version to be compatible.

Please note: Both the roadmap and the release dates reflect intentions rather than firm commitments and may change
as we learn more or encounter unexpected issues. If dates do need to change, we will be as transparent as possible,
and log all changes in the changelog at the bottom of this page.

# Maintenance Policy

For `aws-advanced-jdbc-wrapper` new features and active development always takes place against the newest version.
The `aws-advanced-jdbc-wrapper` project follows the semantic versioning specification for assigning version numbers
to releases, so you should be able to upgrade to the latest minor version of that same major version of the
software without encountering incompatible changes (e.g., 1.1.0 → 1.3.x).

Sometimes an incompatible change is unavoidable. When this happens, the software’s maintainers will increment
the major version number (e.g., increment from `aws-advanced-jdbc-wrapper` 1.1.1 to `aws-advanced-jdbc-wrapper` 2.0.0).
The last minor version of the previous major version of the software will then enter a maintenance window
(e.g., 1.3.x). During the maintenance window, the software will continue to receive bug fixes and security patches,
but no new features.

We follow OpenSSF’s best practices for patching publicly known vulnerabilities, and we make sure that there are
no unpatched vulnerabilities of medium or higher severity that have been publicly known for more than 60 days
in our actively maintained versions.

The duration of the maintenance window will vary from product to product and release to release.
By default, versions will remain under maintenance until the next major version enters maintenance,
or 1-year passes, whichever is longer. Therefore, at any given time, the current major version and
previous major version will both be supported, as well as older major versions that have been in maintenance
for less than 12 months. Please note that maintenance windows are influenced by the support schedules for
dependencies the software includes, community input, the scope of the changes introduced by the new version,
and estimates for the effort required to continue maintenance of the previous version.

The software maintainers will not back-port fixes or features to versions outside the maintenance window.
That said, PRs with said back-ports are welcome and will follow the project's review process.
No new releases will result from these changes, but interested parties can create their own distribution
from the updated source after the PRs are merged.

| Major Version | Latest Minor Version | Status      | Initial Release | Maintenance Window Start | Maintenance Window End |
|---------------|----------------------|-------------|-----------------|--------------------------|------------------------|
| 1             | 1.0.2                | Maintenance | Oct 5, 2022     | Apr 28, 2023             | Apr 28, 2024           | 
| 2             | 2.3.5                | Current     | Apr 28, 2023    | N/A                      | N/A                    | 
