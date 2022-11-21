# dnpcsql

(Distributed Nested Performance Contexts in SQL)

## Motivation

This project originates from work with the realtime monitoring
component of [Parsl](https://parsl-project.org/).

As a complement to Parsl's realtime, dashboard oriented monitoring
system, this project is aimed at people who are comfortable
writing their own analyses of performance data (which is often the
case as Parsl users tend to be scientific programmers); it is intended
to support after-the-fact, non-realtime performance analyses, from
multiple data sources (primarily log files of different components,
the sqlite3 database of parsl monitoring information, but also for
example, data coming from AWS cloudwatch as part of a funcX
side-project)

This project is also intended to support easy integration of new
data sources - in the parsl use case, when new executor components
are added, for example, or when closer investigation of performance
of a particular block of code leads to ad-hoc throwaway measurements
not intended to form part of the parsl mainline code.

## Database

This project emphasises using SQL/relational databases as the data model
and storage system. It is not intended to be a different query system
that happens to use SQL as a backend; instead, the main model should be:
relational queries (primarily SQL) against relational data. That involves
exposing warts on the relational model, rather than hiding them behind
some other model.

## High level model and data flow

The basic object that is being analysed is a "span". This term comes
from the distributed tracing/observability community. For example,
[LightStep](https://docs.lightstep.com/docs/understand-distributed-tracing)
defines a span as:

"A span is a named, timed operation that represents a piece of the workflow."

In the case of parsl, spans include things like: parsl level tasks,
parsl level tries, entire parsl workflows, executor level execution attempts,
batch provider blocks, executor level task executors, task executions on
a particular worker.

At the application level above parsl, there might be spans that represent
the entire execution of an application, and each individual component of an
application workflow; and inside the application code that runs on a worker,
that execution might be divided into spans.

Spans can be nested inside other spans (the N in dnpcsql).
Often spans will be nested in a way that is "almost" a 1-1 relationship: for
example, very often an application level task will correspond with exactly
one parsl task, which will correspond with exactly one parsl try, which
will correspond with exactly one executor level task execution. But that
correspondence is not exact: what happens if a task is never executed? or
if an application level task fails before it is even submitted to parsl?

A span will have multiple events associated with it. For the purposes of
dnpcsql, an event has a timestamp and a type. Usually there is at least
a start and end event. Often the end event will describe how the
span ended - for example, a failure event type or successful completion
event type.

## Analysis flow

An example flow for performance data looks like this:

* stuff happens (that we will want to analyse the performance of) and
events are logged in ad-hoc formats (because many components are used,
this project cannot try to enforce a common reporting format or mechanism,
and instead must accept data as it comes)

* dnpcsql users import revelant information about spans, from various
sources, into an sqlite3 database (so far, usually called `dnpc.sqlite3`)

* users get specific data from the database using SQL queries

* users use matplotlib to plot that data

* users examine plots

The query/plot/examine stages are intended to be quite dynamic, with
users writing and modifying their own code to explore performance data,
rather than a small number of pre-defined plots being made by developers
and released to dnpcsql users. Code in the dnpcrepo around analysis
should look more like helper code, rather than pre-packaged analyses.

## Database schema

Users are free to structure their database schema as they please. There are
three different kinds of structure that have been used with dnpcsql so far:

* span / event / subspan - one table represents all spans, regardless of type;
another represents events in those spans; and a third represents
subspan/superspan relationships. This is span/event type unaware.

* Type-aware tables - eg. parsl monitoring.db, with a table for tasks, another
table for tries, etc. Different columns in the database represent timestamps
for different events (and other data about the types of span)

* raw json log records - for example, from cloudwatch. In this model, SQL
JSON functions are used in queries to access relevant fields at index and
query time, rather than at import time. This leads to more complex queries,
but also allows access of the entirety of a log record.

Data in all of these forms (and other forms) can exist in the sqlite3 database
at once, accessed with SQL queries that understand all of the forms that they
are querying.

## Install

At nersc:

module load python3
conda create --name dnpcsql-analysis python=3.10
conda activate dnpcsql-analysis
pip install .

on subsequent uses, only:

module load python3
conda activate dnpcsql-analysis
