steps:
- name: gcr.io/cloud-builders/git
  args: ['clone', 'https://github.com/FireXStuff/firex-keeper.git']
  dir: '$BUILD_ID'
- name: gcr.io/$PROJECT_ID/firex-alpine-build
  args: ['sh', '-c', 'sudo pip3 install firexbuilder && firex-build all --sudo --output_dir /home/firex --upload_pip_if_tag --upload_codecov --skip_htmlcov']
  dir: '$BUILD_ID/firex-keeper'
  secretEnv: ['TWINE_PASSWORD']

secrets:
- kmsKeyName: projects/firex-kit-documentation/locations/global/keyRings/secrets/cryptoKeys/TWINE_PASSWORD
  secretEnv:
    TWINE_PASSWORD: 'CiQAI+bsJfbKBVxA+tOJAGnHIJRG0zFyUbzjvBx3S0BSF0T3UdUSOgDnI0hgPR5iY1f1pYZJUaPASEjviB8mzBIlAEI0LSMy+r9FUOIKIPDTO4lQN8BWsq3ksNAhe8f5aKA='
