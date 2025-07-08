# CARLA scalability measurements

Contains scripts used to replicate the CARLA scalability and energy consumption measurements presented in our publication.

## Example data

- Folder ``example_output`` contains example data and scripts for plotting the data.
- See ``example_output/plot.ipynb`` for plotting the data.

## Usage instructions

1. Download and install [CARLA](https://carla.org/)
  - Tested with [0.9.14](https://github.com/carla-simulator/carla/releases/tag/0.9.14) in Windows and Linux.
  - No need to build from source, unless you want to modify the environment through Unreal Editor.
  - This version of CARLA needs Python 3.8 to work. It does not support 3.9 or above.
2. Install requirements from ``requirements.txt``
  - ``virtualenv -p python3.8 venv``
  - ``source venv/bin/activate``
  - ``pip install -r requirements.txt``
  - If necessary, also install jupyter (or convert the .ipynb to regular .py):
    - ``pip install jupyter``
    - ``jupyter notebook``
3. Run ``measure.ipynb`` to create your own measurements
4. Plot the results (refer to ``example_output/plot.ipynb`` for an example)

## Troubleshooting

- To get RAPL measurements to work, you may need to run the following command:
  - ``sudo chmod -R a+r /sys/class/powercap/intel-rapl``
  - This adds read permission to the RAPL metrics.