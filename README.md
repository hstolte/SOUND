# SOUND

In the following, we are providing necessary setup steps as well as instructions on how to reproduce the
SOUND experiments. All steps have been tested under **MacOS Ventura 13.6**.

## Setup

### Step 0: Dependencies

The following dependencies are not managed by our automatic setup and need to be installed
beforehand.:

- git
- wget
- maven
- unzip
- OpenJDK 1.8
- python3 (+ pip3)

For Ubuntu 20.04: `sudo apt-get install git wget maven unzip openjdk-8-jdk python3-pip python3-yaml libyaml-dev cython`

Below we assume the repository has been cloned into the directory `repo_dir`. 

### Step 1: Install Python Requirements

- Using a virtual environment is suggested. 
- Initializing the environment and running the experiments requires: `gdown pyyaml tqdm`
- Recreating the figures requires additionally: `pandas matplotlib seaborn numpy adjustText xlsxwriter` 

To install the python requirements automatically, from `repo_dir`, run *one* of the following commands:

```bash
# Full Dependencies (Running Experiments and Plotting)
pip install -r requirements.txt
# Minimal Dependencies (Running Experiments Only)
pip install -r requirements-minimal.txt

```

### Step 2: Automated Setup

This method will automatically download Apache Flink and the input datasets,
and compile the framework.

1. From `repo_dir`, run `./auto-setup.sh`
2. Run `./init-configs.sh` to use the Flink configuration used in our experiments


#### (Alternative) Manual Setup

In case of problems with the automatic setup, you can prepare the environment manually:

1. Download [Apache Flink 1.14.0](https://archive.apache.org/dist/flink/flink-1.14.0/flink-1.14.0-bin-scala_2.11.tgz) and decompress in `repo_dir`
3. Get the input datasets, either using `./get-datasets.sh` or by manually downloading from [here](https://drive.google.com/u/0/uc?id=1uNOUlCoa9CfH7WCxe3nSsCwQVVjf9teB) and decompressing in `repo_dir/data/input`
4. Compile the two experiment jars, from repo dir: `mvn -f helper_pom.xml clean package && mv target/helper*.jar jars; mvn clean package; mvn install`
2. Run `./init-configs.sh` to use the Flink configurations used in our experiments


## Running Experiments

### Automatic Reproduction of the Paper's Experiments and Plots

The `reproduce/sound/` directory contains a script for each evaluation figure of the paper, which will run the experiment automatically, store the results, and create a plot. You need to provide the `#repetitions` and `duration` in minutes as arguments.
For example, to reproduce Figure 5, run (from `repo_dir`):

```bash
# Reproduce Figure 5, left panel, of the paper for 5 repetitions of 1.5 minutes
./reproduce/sound/figure6_left.sh 5 1.5 
```
Results are stored in the folder `data/output`.

*Some experiment scripts print debugging information that
is usually safe to ignore, as long as the figures are generated successfully.*

