# Need fastentrypoints to monkey patch setuptools for faster console_scripts
# noinspection PyUnresolvedReferences
import fastentrypoints
from setuptools import setup, find_packages
import versioneer

setup(name='firex_keeper',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      description='FireX event processor for creating an sqlite DB.',
      url='https://github.com/FireXStuff/firex-keeper',
      author='Core FireX Team',
      author_email='firex-dev@gmail.com',
      license='BSD-3-Clause',
      packages=find_packages(),
      zip_safe=True,
      install_requires=[
            'firexapp',
            'SQLAlchemy',
      ],
      entry_points={
          'console_scripts': [
              'firex_keeper = firex_keeper.__main__:main',
          ],
          'firex_tracking_service':  ['firex_keeper_launcher = firex_keeper.keeper_launcher:FireXKeeperLauncher', ],
      },)
