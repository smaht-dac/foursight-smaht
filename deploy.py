"""
Generate gitignored .chalice/config.json for deploy and then run deploy.
Takes on parameter for now: stage (either "dev" or "prod")
"""

from __future__ import print_function, unicode_literals
import os
import sys
import argparse
import json
import subprocess

CONFIG_BASE = {
  "stages": {
    "dev": {
      "api_gateway_stage": "api",
      "autogen_policy": False,
      "lambda_memory_size": 512,
      "lambda_timeout": 300,
      "environment_variables": {
          "chalice_stage": "dev"
      }
    },
    "prod": {
      "api_gateway_stage": "api",
      "autogen_policy": False,
      "lambda_memory_size": 512,
      "lambda_timeout": 300,
      "environment_variables": {
          "chalice_stage": "prod"
      }
    }
  },
  "version": "2.0",
  "app_name": "foursight"
}


def build_config_and_deploy(stage):
    # key to de-encrypt access key
    akey_secret = os.environ.get("SECRET")
    client_id = os.environ.get("CLIENT_ID")
    client_secret = os.environ.get("CLIENT_SECRET")
    dev_secret = os.environ.get("DEV_SECRET")
    admin = os.environ.get("FS_ADMIN")
    if not (akey_secret and client_id and client_secret and dev_secret):
        print('ERROR. You are missing one more more environment variables needed to deploy Foursight. Need: SECRET, CLIENT_ID, CLIENT_SECRET, DEV_SECRET.')
        sys.exit()
    for curr_stage in ['dev', 'prod']:
        CONFIG_BASE['stages'][curr_stage]['environment_variables']['SECRET'] = akey_secret
        CONFIG_BASE['stages'][curr_stage]['environment_variables']['CLIENT_ID'] = client_id
        CONFIG_BASE['stages'][curr_stage]['environment_variables']['CLIENT_SECRET'] = client_secret
        CONFIG_BASE['stages'][curr_stage]['environment_variables']['DEV_SECRET'] = dev_secret
        CONFIG_BASE['stages'][curr_stage]['environment_variables']['ADMIN'] = admin

    file_dir, _ = os.path.split(os.path.abspath(__file__))
    filename = os.path.join(file_dir, '.chalice/config.json')
    print(''.join(['Writing: ', filename]))
    with open(filename, 'w') as config_file:
        config_file.write(json.dumps(CONFIG_BASE))
    # actually deploy
    subprocess.call(['chalice', 'deploy', '--stage', stage])


def main():
    parser = argparse.ArgumentParser('chalice_deploy')
    parser.add_argument(
        "stage",
        type=str,
        choices=['dev', 'prod'],
        help="chalice deployment stage. Must be one of 'prod' or 'dev'")
    args = parser.parse_args()
    build_config_and_deploy(args.stage)


if __name__ == '__main__':
    main()
