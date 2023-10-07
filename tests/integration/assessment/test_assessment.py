import logging

from databricks.labs.ucx.assessment.crawlers import (
    AzureServicePrincipalCrawler,
    ClustersCrawler,
    JobsCrawler,
    PipelinesCrawler,
)
from databricks.labs.ucx.framework.crawlers import StatementExecutionBackend

logger = logging.getLogger(__name__)


def test_pipeline_crawler(ws, make_pipeline, inventory_schema, sql_backend):

    logger.info("setting up fixtures")
    created_pipeline = make_pipeline(spn_example=1)

    pipeline_crawler = PipelinesCrawler(ws=ws, sbe=sql_backend, schema=inventory_schema)
    pipelines = pipeline_crawler.snapshot()
    results = []
    for pipeline in pipelines:
        if pipeline.success != 0:
            continue
        if pipeline.pipeline_id == created_pipeline.pipeline_id:
            results.append(pipeline)

    assert len(results) >= 1
    assert results[0].pipeline_id == created_pipeline.pipeline_id


def test_cluster_crawler(
    ws,
    make_cluster,
    inventory_schema,
    sql_backend
):
    created_cluster = make_cluster(single_node=True, spn_example=1)
    new_cluster = created_cluster.result()
    cluster_crawler = ClustersCrawler(ws=ws, sbe=sql_backend, schema=inventory_schema)
    clusters = cluster_crawler.snapshot()
    results = []
    for cluster in clusters:
        if cluster.success != 0:
            continue
        if cluster.cluster_id == new_cluster.cluster_id:
            results.append(cluster)

    assert len(results) >= 1
    assert results[0].cluster_id == new_cluster.cluster_id


def test_job_crawler(ws, make_job, inventory_schema, sql_backend):

    new_job = make_job(spn_example=1)
    job_crawler = JobsCrawler(ws=ws, sbe=sql_backend, schema=inventory_schema)
    jobs = job_crawler.snapshot()
    results = []
    for job in jobs:
        if job.success != 0:
            continue
        if int(job.job_id) == new_job.job_id:
            results.append(job)

    assert len(results) >= 1
    assert int(results[0].job_id) == new_job.job_id


def test_spn_crawler(ws, inventory_schema, make_job, make_pipeline, sql_backend):
    make_job(spn_example=1)
    make_pipeline(spn_example=1)
    spn_crawler = AzureServicePrincipalCrawler(ws=ws, sbe=sql_backend, schema=inventory_schema)
    spns = spn_crawler.snapshot()
    results = []
    for spn in spns:
        results.append(spn)

    assert len(results) >= 2
    assert results[0].storage_account == "storage_acct_1"
    assert results[0].tenant_id == "directory_12345"
