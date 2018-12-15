import os

from setuptools import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

def run_setup():
    setup(
	  name='rabbitmq-metrics-to-graphite',
	  version='0.2',
	  description='https://github.com/orachide/rabbitmq-metrics-to-graphite',
	  keywords = 'RabbitMQ Graphite Metrics',
	  url='https://github.com/orachide/rabbitmq-metrics-to-graphite',
	  download_url = 'https://github.com/orachide/rabbitmq-metrics-to-graphite/archive/0.2.tar.gz',
	  author='Rachide Ouattara',
	  author_email='ouattchidi@gmail.com',
	  license='BSD',
	  packages=['rabbitmq_metrics_to_graphite'],
	  install_requires=[
		    'pyrabbit'
	  ],
	  zip_safe=True,
	  classifiers=[
	   ],
      entry_points="""
      [console_scripts]
      rabbitmq-metrics-to-graphite=rabbitmq_metrics_to_graphite:main
      """,
	)
if __name__ == '__main__':
    run_setup()
