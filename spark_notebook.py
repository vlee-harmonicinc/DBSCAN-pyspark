#!/usr/bin/env python

# https://github.com/felixcheung/vagrant-projects

from os import chdir, getenv, system, umask
from os.path import exists, expanduser
from subprocess import check_output, check_call
from sys import executable


def setenv(var, value, overwrite=True):
    from os import environ
    if overwrite or not var in environ:
        environ[var] = value


#-----------------------
# PySpark
#

master = 'local[*]'

num_executors   = 12  # 24
executor_cores  = 2
executor_memory = '1g'  # 10g
driver_memory = '1536M'

umask(0077)  # ensure that always chmod go-wrx
chdir(expanduser("~"))

# Vivian modify to import graphframa
# print getenv('PYSPARK_SUBMIT_ARGS') #checked, it is None
# since graphframes support pyspark since spark 1.4, update spark from 1.3 to 1.6(since the oldest version can be download is 1.6)
# modify enviorment variable $SPARK_HOME to new downloaded spark 1.6 
# export SPARK_HOME="/usr/local/bin/spark-1.6.3-bin-hadoop2.6"
# export PATH=/home/vagrant/hadoop-2.6.5/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/usr/local/bin/spark-1.6.3-bin-hadoop2.6/bin
tmp_submit_args='--packages graphframes:graphframes:0.5.0-spark1.6-s_2.11 --driver-memory %s --num-executors %d --executor-cores %d --executor-memory %s' % (driver_memory, num_executors, executor_cores, executor_memory)
setenv('PYSPARK_SUBMIT_ARGS',tmp_submit_args )

default_pyspark_submit_args = '--driver-memory %s --num-executors %d --executor-cores %d --executor-memory %s' % (driver_memory, num_executors, executor_cores, executor_memory)
pyspark_submit_args = getenv('PYSPARK_SUBMIT_ARGS', None) or default_pyspark_submit_args
pyspark_submit_args = '--master %s %s' % (master, pyspark_submit_args)

setenv('PYSPARK_PYTHON', executable, overwrite=False)
setenv('PYSPARK_DRIVER_PYTHON', 'ipython')  # PySpark Driver (ie. IPython)
profile_name = 'pyspark'
setenv('PYSPARK_DRIVER_PYTHON_OPTS', 'notebook --profile=%s' % profile_name)

#-----------------------
# IPython Notebook
#

ipython_notebook_config_template = '''c = get_config()
c.NotebookApp.ip = '{ip}'
c.NotebookApp.port = {port}
c.NotebookApp.open_browser = False
'''

pyspark_setup_template = '''from os import getenv
if not getenv('PYSPARK_SUBMIT_ARGS', None):
    raise ValueError('PYSPARK_SUBMIT_ARGS environment variable is not set')

spark_home = getenv('SPARK_HOME', None)
if not spark_home:
    raise ValueError('SPARK_HOME environment variable is not set')
'''

pyspark_custom_css = '''
.rendered_html img {
    display: inline-block;
}

.rendered_html h1 {
    font-family: "Source Sans Pro",sans-serif;
    font-weight: 200;
    line-height: 36px;
    font-size: 33px
}

.rendered_html h2,h2 .job-name {
    font-family: "Source Sans Pro",sans-serif;
    font-weight: 200;
    line-height: 30px;
    font-size: 27px
}

.rendered_html h3 {
    font-family: "Source Sans Pro",sans-serif;
    font-weight: 200;
    line-height: 24px;
    font-size: 22px
}

.rendered_html h4 {
    font-family: "Source Sans Pro",sans-serif;
    font-weight: 200;
    line-height: 21px;
    font-size: 18px
}

.rendered_html h5 {
    font-family: "Source Sans Pro",sans-serif;
    font-weight: 200;
    line-height: 19px;
    font-size: 16px
}
'''

pyspark_custom_js = ''

matplotlib_inline = '''
c.IPKernelApp.matplotlib = 'inline'
c.InlineBackend.rc = {}
'''

ip = '*'  # Warning: this is potentially insecure
port = 8001

#-----------------------
# Create profile and start
#

try:
    ipython_profile_path         = check_output(['ipython', 'locate']).rstrip('\n') + '/profile_%s' % profile_name
    setup_py_path                = ipython_profile_path + '/startup/00-pyspark-setup.py'
    ipython_notebook_config_path = ipython_profile_path + '/ipython_notebook_config.py'
    ipython_kernel_config_path   = ipython_profile_path + '/ipython_kernel_config.py'
    ipython_custom_css_path      = ipython_profile_path + '/static/custom/custom.css'
    ipython_custom_js_path       = ipython_profile_path + '/static/custom/custom.js'

    if not exists(ipython_profile_path):
        print 'Creating IPython Notebook profile\n'
        check_call(['ipython', 'profile', 'create', profile_name])
        print 'Writing custom CSS/JS\n'
        with open(ipython_custom_css_path, 'w') as css_file:
            css_file.write(pyspark_custom_css)
        with open(ipython_custom_js_path, 'w') as js_file:
            js_file.write(pyspark_custom_js)
        print '\n'

    if not exists(setup_py_path):
        print 'Writing PySpark setup\n'
        with open(setup_py_path, 'w') as setup_file:
            setup_file.write(pyspark_setup_template)

    # matplotlib inline
    kernel_config = open(ipython_kernel_config_path).read()
    if "c.IPKernelApp.matplotlib = 'inline'" not in kernel_config:
        print 'Writing IPython kernel config\n'
        new_kernel_config = kernel_config.replace('# c.IPKernelApp.matplotlib = None', matplotlib_inline)
        with open(ipython_kernel_config_path, 'w') as kernel_file:
            kernel_file.write(new_kernel_config)

    if not exists(ipython_notebook_config_path) or 'open_browser = False' not in open(ipython_notebook_config_path).read():
        print 'Writing IPython Notebook config\n'
        with open(ipython_notebook_config_path, 'w') as config_file:
            config_file.write(ipython_notebook_config_template.format(ip=ip, port=port))

    print 'Launching PySpark with IPython Notebook\n'
    cmd = 'pyspark %s' % pyspark_submit_args
    system(cmd)
    exit(0)

except KeyboardInterrupt:
    print 'Aborted\n'
    exit(1)
