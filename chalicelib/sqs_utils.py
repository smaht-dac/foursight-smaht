from foursight_core.sqs_utils import SQS as SQS_from_core
from .stage import Stage as CurrentStageClass


class SQS(SQS_from_core):
    Stage = CurrentStageClass
