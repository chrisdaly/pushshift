from setuptools import setup


def readme():
    with open('README.rst') as f:
        return f.read()


setup(name='pushshift',
      version='0.1',
      description="Interacts with the Pushshift Reddit API to retrieve submission and comment data.",
      long_description=readme(),
      url='https://github.com/chrisdaly/pushshift',
      author='Chris Daly',
      author_email='cdaly@w2ogroup.com',
      license='MIT',
      packages=['pushshift'],
      install_requires=['requests', 'tqdm', 'pandas', ],  # 'lxml'
      zip_safe=False,
      test_suite='nose.collector',
      tests_require=['nose']
      )
