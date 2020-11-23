from conftests import *


class TestEnvironment():
    def test_list_environments(self):
        env_list = environment.Environment().list_environment_names()
        # assume we have at least one environments
        assert (isinstance(env_list, list))
        assert (self.environ in env_list)

