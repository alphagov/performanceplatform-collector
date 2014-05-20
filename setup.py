import io
import os
import re
from setuptools import setup, find_packages

from performanceplatform import collector


class Setup(object):

    """A series of helpers for getting the setup looking buff"""

    @staticmethod
    def read(fname, fail_silently=False):
        """
        Read the content of the given file. The path is evaluated from the
        directory containing this file.
        """
        try:
            filepath = os.path.join(os.path.dirname(__file__), fname)
            with io.open(filepath, 'rt', encoding='utf8') as f:
                return f.read()
        except:
            if not fail_silently:
                raise
            return ''

    @staticmethod
    def version():
        data = Setup.read(
            'performanceplatform/collector/__init__.py'
        )
        version = re.search(
            r"^__VERSION__ = ['\"]([^'\"]*)['\"]",
            data,
            re.M | re.I
        ).group(1).strip()
        return version

    @staticmethod
    def requirements(fname):
        """
        Create a list of requirements from the output of the pip freeze command
        saved in a text file.
        """
        packages = Setup.read(fname, fail_silently=True).split('\n')
        packages = (p.strip() for p in packages)
        packages = (p for p in packages if p and not p.startswith('#'))
        return list(packages)

    @staticmethod
    def long_description():
        return Setup.read('README.rst')

setup(
    name='performanceplatform-collector',
    version=collector.__VERSION__,
    packages=find_packages(exclude=['test*']),

    # metadata for upload to PyPI
    author=collector.__AUTHOR__,
    author_email=collector.__AUTHOR_EMAIL__,
    maintainer='Government Digital Service',
    url='https://github.com/alphagov/performanceplatform-collector',

    description='performanceplatform-collector: tools for sending'
        'data to the Performance Platform',
    long_description=Setup.long_description(),
    license='MIT',
    keywords='api data performance_platform',

    install_requires=Setup.requirements('requirements.txt'),
    tests_require=Setup.requirements('requirements_for_tests.txt'),

    test_suite='nose.collector',

    entry_points={
        'console_scripts': [
            'pp-collector=performanceplatform.collector.main:main'
        ]
    }
)
