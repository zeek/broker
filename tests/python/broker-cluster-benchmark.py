import os, sys, zipfile, subprocess, shutil

from subprocess import PIPE

# -- setup and cleanup --------------------------------------------------------

test_recording = 'zeek-dns-traffic-recording'

class Environment:
    def __init__(self, executable_path):
        self.exe = executable_path
        self.test_dir = os.environ.get('BROKER_TEST_DIR')
        if not os.path.isdir(self.test_dir):
            raise RuntimeError('environment variable BROKER_TEST_DIR is not a valid directory')
        self.input_dir = os.path.join(self.test_dir, '.tmp', 'broker-cluster-benchmark')
        self.recording_dir = os.path.join(self.input_dir, test_recording)

    def __enter__(self):
        self.clear_environment()
        self.prepare_environment()
        return self

    def __exit__(self, type, value, tb):
        self.clear_environment()

    def prepare_environment(self):
        os.makedirs(self.input_dir)
        file_name = test_recording + '.zip'
        file_path = os.path.join(self.test_dir, 'integration', file_name)
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(self.input_dir)
        with open(os.path.join(self.recording_dir, 'expected-tpl.conf'), mode='r') as f:
            format_vars = {'path': self.recording_dir}
            self.expected = f.read() % format_vars

    def clear_environment(self):
        if os.path.isdir(self.input_dir):
            shutil.rmtree(self.input_dir)

# -- integration testing ------------------------------------------------------

def test_config_generation(exe, recording_dir, expected):
    dirs = [f.path for f in os.scandir(recording_dir) if f.is_dir()]
    cmd = [exe, '--caf.logger.console.verbosity=quiet', '--mode=generate-config'] + dirs
    with subprocess.Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True, universal_newlines=True) as proc:
        output, errors = proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError('failed to generate config: ' + errors)
        if output != expected:
            sys.stderr.write('*** ERROR: generate-config procuded wrong result\n')
            sys.stderr.write('\n*** EXPECTED:\n')
            sys.stderr.write(expected)
            sys.stderr.write('\n*** GOT:\n')
            sys.stderr.write(output)
            sys.exit(1)
        print('test_config_generation: pass')
        return output

def run_benchmark(exe, config):
    cmd = [exe, '--caf.logger.console.verbosity=quiet', '--cluster-config-file=-']
    with subprocess.Popen(cmd, stdout=PIPE, stderr=PIPE, stdin=PIPE, close_fds=True, universal_newlines=True) as proc:
        proc.stdin.write(config)
        proc.stdin.close()
        proc.stdin = None
        output, errors = proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError('failed to run the benchmark: ' + errors)
        print('run_benchmark: pass')
        sys.stdout.write('*** Benchmark output:\n')
        sys.stdout.write(output)


# -- main ---------------------------------------------------------------------

if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise RuntimeError('expected exactly one argument: path to broker-cluster-benchmark')
    with Environment(sys.argv[1]) as e:
        config = test_config_generation(e.exe, e.recording_dir, e.expected)
        run_benchmark(e.exe, config)
