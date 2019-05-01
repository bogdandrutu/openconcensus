# OpenTelemetry
[![Gitter chat][gitter-image]][gitter-url]
[![Build Status][travis-image]][travis-url]

OpenTelemetry is a working name of a combined OpenCensus and OpenTracing
project.

## This codebase will be ready for review April 24th, 2019. 

## Please note that this is a temorary repository, and we cannot accept PRs until the project is moved to its offical home, where it can be covered by the CNCF CLA.

We would love to hear from the larger community: please provide feedback
proactively on the [OpenCensus
gitter](https://gitter.im/census-instrumentation/Lobby), [OpenTracing
gitter](https://gitter.im/opentracing/public) or file [an
issue](/issues). As we continue to
make progress, we will post updates to the OpenCensus and OpenTracing blogs.

## Plan

[Please review the roadmap here](https://medium.com/opentracing/a-roadmap-to-convergence-b074e5815289).

In the coming months [we plan to merge the OpenCensus and OpenTracing
projects](https://medium.com/opentracing/merging-opentracing-and-opencensus-f0fe9c7ca6f0).
The technical committee will drive the merge effort. We’ve identified areas that
require deeper discussion and areas that merely require alignment around
terminology and usage patterns.

We have a three-step plan for the project merge: 

1. spike of merged interfaces,
2. beta release of new project binaries, and
3. kicking off the work towards a 1.0 release.

### Spike of merged interfaces

Spike API merge will happen in a separate repository, focused on Java
specifically. The main goal of the spike is to make sure that we have a clear
path forward, and that there will be no unforeseen technical issues blocking the
merge of the projects (while staying true to our  declared goals for the merge).

As a result of the spike we plan to produce:

- Alpha version of a merged interface in new repository.
- Rough port of OpenCensus implementation as an implementation of choice for
  this API.
- Rough OpenTracing bridge to new interface.
- Supplemental documentation and design documents

We expect this spike will take us a few weeks.

### Beta release of a new project

Once we have cleared out the path - we plan to initiate a transition of active
contribution and discussions from OpenCensus and OpenTracing to the new project.
We will

- Clean up OpenCensus into official SDK of new project
- Release an official OpenTracing bridge to new Interface

We will minimize the duration of this transition so that users can switch to the
new API as seamlessly as possible, and contributors can quickly ensure that
their work is compatible with future versions. We will also encourage all
contributors to start working in the new project once the merger announced. So
there will be no time of duplicative contributions.

### Kick off the work towards 1.0

After beta release we will encourage customers and tracing vendors to start
using the new project, providing feedback as they go. So we can ensure a high
quality v1.0 for the merged project:

- We will allow ourselves to break *implementations*, but not people using the
  public Interfaces.
- Additions (into interfaces for instance) will involve a best-effort attempt at
  backwards compatibility (again, for implementations – callers of the public
  APIs should not be negatively affected by these additions).

### Summary

We plan to merge projects and pave the path for future improvements as a unified
community of tracing vendors, users and library authors who wants apps be
managed better. We are open to feedback and suggestions from all of you!

[travis-image]: https://travis-ci.org/bogdandrutu/openconsensus.svg?branch=master
[travis-url]: https://travis-ci.org/bogdandrutu/openconsensus 
[gitter-image]: https://badges.gitter.im/census-instrumentation/big-hook.svg 
[gitter-url]: https://gitter.im/census-instrumentation/big-hook?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge
