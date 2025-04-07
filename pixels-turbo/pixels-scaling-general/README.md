## Pixels Auto-Scaling

Auto-scaling uses the query data collected by trino to predict resource usage (e.g., cpu, memory), and expands resources (number of workers) in advance based on the prediction results to cope with the upcoming load.

## Components

Currently, auto-scaling mainly consists of three parts:

- **data collection**: When trino executes the query, it will collect the resource usage and other data of the query. This part will send a request to `trino-server` and at the end of the query, the resource usage data will be transmitted to pixels. Pixels will persist the data locally and use it as historical data for forecasting.
- **forecast:** This part uses the data collected in the past week, divides the queries into different levels of queries according to CPU usage, and uses AutoARIMA to predict each level of queries. The prediction granularity is 5 minutes. This work will be repeated for memory.
- **scaling:** This part will use the cpu and memory usage predicted by forecast to expand resources. After getting the resource usage,  we just need to expand the resources to the predicted results.

## Use Auto-Scaling

To use Auto-Scaling, you need:

- Configure your python environment (install duckdb, statsforecast).
- Refer to [pixels-ec2-scaling](https://github.com/slzo/pixels/tree/master/pixels-turbo/pixels-scaling-ec2) to create your startup template, which is used to expand workers

Modify `PIXELS_HOME/etc/pixels.properties`:

- The pixels-scaling strategy is modified to `AIAS`
- Specify your launch template
- Specify your python environment
- cpuspl and memspl are variable options, used to adjust your split threshold and number of levels for query levels, you can modify it according to your workload.

```properties
## choose auto scaling policy
vm.auto.scaling.policy=AIAS
# launch template id of pixels worker
vm.lt.id=lt-xxxxxx

# choose the suit python path
# if you use global python instead of python-venv, you can easily modify it to python
python.env.path = /home/ubuntu/dev/venvpixels/bin/python
# split cputime (ms)
cpuspl = [10000,60000,300000,600000]
# split mem (G)
memspl = [1,8,16,32,64]
```