import logging
from datetime import datetime
import time

from airflow import DAG, settings
from airflow.models import BaseOperator, DagBag
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from airflow.operators.sensors import BaseSensorOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.subdag_operator import SubDagOperator


log = logging.getLogger(__name__)

gs_dag = DAG(
    "gs_dag", 
    description="Skynet BAU DAG",
    schedule_interval="0 12 * * *",
    start_date=datetime(2017, 3, 20), 
    catchup=False)

batch_dag = DAG(
    "batch_dag",
    description="Skynet BAU DAG",
    start_date=datetime(2017, 3, 20), 
    schedule_interval=None)

class DocInterfaceOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(DocInterfaceOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("executing doc interface operator")
        return True


class ExtractorSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(ExtractorSensor, self).__init__(*args, **kwargs)


    def poke(self, context):
        log.info("executing extractor sensor")

        #current_minute = datetime.now().minute
        #if current_minute % 3 != 0:
        #    log.info("Current minute (%s) not is divisible by 3, sensor will retry.", current_minute)
        #    return False
        #
        #log.info("Current minute (%s) is divisible by 3, sensor finishing.", current_minute)
        return True


class OCRSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(OCRSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        log.info("executing OCR sensor")

        # Check the database for OCR
        # If there is an OCR document, pull it
        # Send it to the OCR server

        #current_minute = datetime.now().minute
        #if current_minute % 3 != 0:
        #    log.info("Current minute (%s) not is divisible by 3, sensor will retry.", current_minute)
        #    return False
        #
        #log.info("Current minute (%s) is divisible by 3, sensor finishing.", current_minute)
        return True

doc_op = DocInterfaceOperator(
            task_id="doc_interface",
            dag=batch_dag
        )
ocr_sensor = OCRSensor(
            task_id="ocr_sensor",
            dag=batch_dag
        )
extractor_sensor = ExtractorSensor(
            task_id="extractor_sensor",
            dag=batch_dag
        )

doc_op.set_downstream(ocr_sensor)
ocr_sensor.set_downstream(extractor_sensor)




class PreProcessingOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(PreProcessingOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('executing preprocessor')

        # the method to obtain the documents can be custom
        # but what it should do is to make sure it takes all the documents
        # and retreives the families and everything required
        # to then store them in the database so the remaining tasks can send them over
        # to perform other things

        # here the logic will be as follows
        # 1) hit the pickwick api for the initial documents
        # 2) resolve metadata for that data, and then pull the families
        # 3) repeat step 2 if required for documents pulled
        # 4) store the documents under the relevant job id

        task_instance = context['task_instance']
        task_instance.xcom_push('job_id', 1)

        for i in range(10):
            run_id = 'trig__' + datetime.utcnow().isoformat()
            session = settings.Session()
            dbag = DagBag(settings.DAGS_FOLDER)
            trigger_dag = dbag.get_dag("batch_dag")

            dr = trigger_dag.create_dagrun(
                run_id=run_id,
                state=State.RUNNING,
                conf=None,
                external_trigger=True)

            log.info("Creating DagRun %s", dr)
            session.add(dr)
            session.commit()
            session.close()
            time.sleep(1)

preprocess_operator = PreProcessingOperator(
    task_id="preprocessor",
    dag=gs_dag
)


