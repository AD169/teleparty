I chose Clickhouse after some research on the internet. But faced many issues with it on jdbc connection and spark connector.Wasted a full day on it.
Then with recomendation from Gemini Flash 5, I switched to Apache Doris. Its setup as done using CURSOR as I don't have good knowledge of Docker and its configurations.
But the memory demanding of Doris is too high even for initiating the server. And my current system does not support it.
Then I switched to Apache Pinot. Its setup as done using Docker compose. And it was working fine. But I faced issues with the ingestion of data due to timeouts and memory issues.
My work experience with OLAP and local development is limited.
All the python and spark codes were written by me.
The shell scripts were created by CURSOR with imputs from me. 
run_pipeline.sh can help load data into parquets using spark and into pinot using python.
run_pipeline_test.sh can help run the benchmark tests for pyspark and pinot.
I ran the benchmarks of spark for 4 usecases but unable do so for pinot. The spark results and queries can be found in testing/test_usecases.sql.

## Note:
Thanks for this exercise. It was a great learning experience for me on docker and shell scripting. I could get much of OLAP because of bottlenecks in the system.