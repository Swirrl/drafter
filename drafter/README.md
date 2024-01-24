# Drafter 2

## Description

A RESTful Clojure web service to support <abbr title="Publish My Data">PMD</abbr>'s admin tool in moving data updates between draft and live triple stores.

## Table of Contents

For details on a particular topic refer to

- [What is Drafter?](doc/what-is-drafter.md) - high level explanation (_draft_)
- [Configuring Drafter](doc/drafter-configuration.md)
- [Developing Drafter](doc/developing-drafter.md) - developing, testing and releasing Drafter
- [Using Drafter as a Service (API Docs)](doc/drafter-service.md)
- [Continuous Integration](doc/ci.md)

---

## Prerequisites

Below is a list of services (both internal and external) and software required to run and develop Drafter.

### Services

- Triplestore
	- **Description**: Data storage
	- **Used for**: Storing the data. We currently use the Stardog database, but any SPARQL 1.1 compliant Triplestore or Endpoint should work.
- CircleCI
	- **Description**: A SAAS cloud hosted continuous integration service
	- **Used for**: Running our test suite and build processes
- auth0 (option)
	- **Description**: External authentication service
	- **Used for**: Managing a database of users and their permissions and supporting authn/z for users and machine-to-machine applications. Drafter provides m2m access to Muttnik (a web interface) and the data-admin workflow.
- MongoDB (option)
	- **Description**: User account database
	- **Used for**: Storing user account and authentication data. Mongodb is an alternative to using auth0; however, auth0 is the recommended option.
- Datadog (production environment only ☁️)
	- **Description**: Cloud monitoring as a service.
	- **Used for**: Gathering metrics, logging and monitoring

### Permissions/Secrets

- Google Cloud Artifact Registry (*if using Docker*)
	- **Description**: Docker container registry in the cloud
	- **Used for**: Storing/fetching our docker container images
	- **For access**: Speak to @leekitching, @rickmoynihan, @andrewmcveigh or @ricroberts on slack.
- auth0
	- **Description**: External authentication service
	- **Used for**: See auth0 in [Services](#Services)
	- **For access**: Speak to @leekitching, @rickmoynihan, @andrewmcveigh or @ricroberts on slack.
- Github
	- **Description**: Project/Code hosting.
	- **Used for**: Some source dependencies are specified as git URLs, and access to those projects is required
	- **For access**: Speak to @rickmoynihan, @andrewmcveigh or @ricroberts on slack.
- CircleCI
	- **Description**: Continuous Integration service
	- **For access**: Providing you have sufficient access via github, you should be able to sign in to circleci with your github account via OAUTH.

### Software

- Java JDK 8 (for x86)
	- **Description**: Java Runtime version required by Stardog (*Note*: it must be x86 version even on Apple Silicon + Rosetta, it won't run with ARM64 JDK)
	- **Used for**: running Stardog (without Docker)
- Java JDK 17 (11+ may work too)
	- **Description**: Java Development Kit for your hardware/OS platform (x86/linux or ARM64/macos)
	- **Used for**: building, testing & running Drafter
- Clojure CLI (a recent release is usually best - at least [1.11.1.1105](https://clojure.org/releases/tools#v1.11.1.1105))
	- **Description**: Clojure command line tools
	- **Used for**: building, testing & running Drafter
- Docker (*if managing processes manually*)
	- **Description**: Container framework
	- **Used for**: Running Stardog & Drafter services.
- Rosetta (if running on Apple Silicon)
	- **Decription**: Allows to run x86 code on Apple Silicon.
	- **Used for**: running Stardog (via x86 JVM) and Docker

---

[omni-repo]:https://github.com/Swirrl/omni
