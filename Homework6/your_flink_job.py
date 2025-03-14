from pyflink.datastream import StreamExecutionEnvironment

# Set up the environment
env = StreamExecutionEnvironment.get_execution_environment()

# Define a simple source (using a list)
data = env.from_collection([1, 2, 3, 4, 5])

# Perform a simple map transformation
data = data.map(lambda x: x * 2)

# Print the result to stdout
data.print()

# Execute the job
env.execute("PyFlink Simple Job")
