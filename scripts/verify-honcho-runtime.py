#!/usr/bin/env python3
"""Verify the installed Honcho SDK and the executable, isolated CLI."""
from importlib.metadata import requires, version
import json
from pathlib import Path
import subprocess


def main():
    import honcho
    from packaging.requirements import Requirement
    from packaging.markers import default_environment

    assert version('honcho-ai') == '2.5.1'
    assert callable(honcho.Honcho)
    # The SDK deliberately reuses Agent dependencies: fail on any incompatible
    # requirement rather than upgrading unrelated Agent packages implicitly.
    environment = default_environment()
    environment['extra'] = ''
    for raw in requires('honcho-ai') or []:
        requirement = Requirement(raw)
        if requirement.marker and not requirement.marker.evaluate(environment):
            continue
        assert version(requirement.name) in requirement.specifier, raw
    versions = json.loads(subprocess.check_output([
        '/opt/honcho-cli/bin/python', '-c',
        "import json; from importlib.metadata import version; print(json.dumps({p:version(p) for p in ['honcho-ai','honcho-cli']}))"
    ], text=True))
    assert versions == {'honcho-ai': '2.5.1', 'honcho-cli': '0.2.0'}, versions
    assert Path('/usr/local/bin/honcho').resolve() == Path('/opt/honcho-cli/bin/honcho')
    subprocess.run(['/usr/local/bin/honcho', '--help'], check=True, capture_output=True, text=True)
    print('Honcho runtime SDK 2.5.1; CLI 0.2.0 with SDK 2.5.1: passed')


if __name__ == '__main__':
    main()
