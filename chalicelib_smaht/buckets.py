import boto3
import json
from chalicelib_smaht.vars import FOURSIGHT_PREFIX
from foursight_core.buckets import Buckets as Buckets_from_core


class Buckets(Buckets_from_core):
    """create and configure buckets for foursight"""

    prefix = FOURSIGHT_PREFIX
    envs = ['smaht', 'data', 'staging', 'smaht-production-green', 'smaht-production-blue']

    def ff_url(self, env):
        if env in ['smaht', 'data']:
            return 'https://data.smaht.org/'
        elif env == 'staging':
            return 'https://staging.smaht.org/'
        else:
            raise Exception(f'Do not load foursight buckets from hidden env name {env}')

    def es_url(self, env):
        raise Exception('This function should not be used')


def main():
    buckets = Buckets()
    buckets.create_buckets()
    buckets.configure_env_bucket()


if __name__ == '__main__':
    main()
