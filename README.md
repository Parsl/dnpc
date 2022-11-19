# dnpcsql

(Distributed Nested Performance Contexts in SQL)

## Motivation

This project originates from work with the realtime monitoring
component of [Parsl](https://parsl-project.org/).

As a complement to Parsl's realtime, dashboard oriented monitoring
system, this project is aimed at people who are comfortable
writing their own analyses of performance data (which is often the
case as Parsl users tend to be scientific programmers).

This project is also intended to support easy integration of new
data sources - in the parsl use case, when new executor components
are added, for example, or when closer investigation of performance
of a particular block of code leads to ad-hoc throwaway measurements
not intended to form part of the parsl mainline code.


## Install

At nersc:

module load python3
conda create --name dnpcsql-analysis python=3.10
conda activate dnpcsql-analysis
pip3 install matplotlib
pip3 install sqlalchemy
pip3 install scipy

on subsequent uses, only:

module load python3
conda activate dnpcsql-analysis
