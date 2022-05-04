import os
import subprocess
import tempfile

def run_zeek_path():
    base = os.path.realpath(__file__)
    for d in (os.path.join(os.path.dirname(base), "../../build"), os.getcwd()):
        run_zeek_script = os.path.abspath(os.path.join(d, "tests/python/run-zeek"))
        if os.path.exists(run_zeek_script):
            return run_zeek_script

    return "zeek" # Hope for the best ...

def run_zeek(script, port):
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".zeek", delete=False) as tmp:
            print(script.replace("__PORT__", str(port)), file=tmp)
            tmp.close()
            subprocess.check_call([run_zeek_path(), "-b", "-B", "broker", tmp.name])
        return True
    except subprocess.CalledProcessError:
        return False
