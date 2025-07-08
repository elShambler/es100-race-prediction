# eastern-states-pace-predict

[![Powered by Kedro](https://img.shields.io/badge/powered_by-kedro-ffc900?logo=kedro)](https://kedro.org)

## Overview

This project is an analysis of the [Eastern States 100](https://www.easternstates100.com) 100-mile foot race through northeastern PA.
The race began in 2015, and over the many years of its runnning, there have many years in which data was recorded of the time at which
runners arrived into different Aid Stations. This is an attempt to capture what data we have.

### Eastern States 100 History

| Year | Run | Splits | Note |
|------|:---:|:------:|------|
| 2014 | [X] | -      | Inagural year |
| 2015 | [X] | -      | No splits recorded |
| 2016 | [X] | [X]    | Splits from UltraLive.net (time-in only) |
| 2017 | [X] | [X]    | Splits from UltraLive.net (time-in only) |
| 2018 | -   | -      | Not run due to safety issues |
| 2019*| [X] | -      | No splits (*fastest time set) |
| 2020 | -   | -      | COVID Year (a few folks ran it anyway) |
| 2021 | [X] | [X]    | Standard set for splits|
| 2022 | [X] | [X]    | Standard |
| 2023 | [X] | [X]    | Standard |
| 2024 | [X] | [X]    | Not run due to flooding |

### Timing Structure
Data for timing comes in two different methods: splits and finish times.
- The finish times contains only information for runners that finished and is the official timing of the race based on runner's chips
- Split data is based on hand recorded data for a specific runner (recorded by bib) into aid stations



## Project Structure
This is your new Kedro project with Kedro-Viz setup, which was generated using `kedro 0.19.3`.

Take a look at the [Kedro documentation](https://docs.kedro.org) to get started.

## Rules and guidelines

In order to get the best out of the template:

* Don't remove any lines from the `.gitignore` file we provide
* Make sure your results can be reproduced by following a [data engineering convention](https://docs.kedro.org/en/stable/faq/faq.html#what-is-data-engineering-convention)
* Don't commit data to your repository
* Don't commit any credentials or your local configuration to your repository. Keep all your credentials and local configuration in `conf/local/`

## How to install dependencies

Declare any dependencies in `requirements.txt` for `pip` installation.

To install them, run:

```
pip install -r requirements.txt
```

## How to run your Kedro pipeline

You can run your Kedro project with:

```
kedro run
```

## How to test your Kedro project

Have a look at the files `src/tests/test_run.py` and `src/tests/pipelines/data_science/test_pipeline.py` for instructions on how to write your tests. Run the tests as follows:

```
pytest
```

To configure the coverage threshold, look at the `.coveragerc` file.

## Project dependencies

To see and update the dependency requirements for your project use `requirements.txt`. Install the project requirements with `pip install -r requirements.txt`.

[Further information about project dependencies](https://docs.kedro.org/en/stable/kedro_project_setup/dependencies.html#project-specific-dependencies)

## How to work with Kedro and notebooks

> Note: Using `kedro jupyter` or `kedro ipython` to run your notebook provides these variables in scope: `catalog`, `context`, `pipelines` and `session`.
>
> Jupyter, JupyterLab, and IPython are already included in the project requirements by default, so once you have run `pip install -r requirements.txt` you will not need to take any extra steps before you use them.

### Jupyter
To use Jupyter notebooks in your Kedro project, you need to install Jupyter:

```
pip install jupyter
```

After installing Jupyter, you can start a local notebook server:

```
kedro jupyter notebook
```

### JupyterLab
To use JupyterLab, you need to install it:

```
pip install jupyterlab
```

You can also start JupyterLab:

```
kedro jupyter lab
```

### IPython
And if you want to run an IPython session:

```
kedro ipython
```

### How to ignore notebook output cells in `git`
To automatically strip out all output cell contents before committing to `git`, you can use tools like [`nbstripout`](https://github.com/kynan/nbstripout). For example, you can add a hook in `.git/config` with `nbstripout --install`. This will run `nbstripout` before anything is committed to `git`.

> *Note:* Your output cells will be retained locally.

[Further information about using notebooks for experiments within Kedro projects](https://docs.kedro.org/en/develop/notebooks_and_ipython/kedro_and_notebooks.html).
## Package your Kedro project

[Further information about building project documentation and packaging your project](https://docs.kedro.org/en/stable/tutorial/package_a_project.html).
