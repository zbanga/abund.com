import os
import posixpath

from celery import shared_task

IB_HOME = os.getenv('IB_HOME', '')

# download and extract TWS jar file
cmd_install = """\
IB_HOME=%(IB_HOME)s
mkdir $IB_HOME
cd $IB_HOME
wget https://download2.interactivebrokers.com/download/unixmacosx_latest.jar
jar xf unixmacosx_latest.jar
"""

# Command to install TWS from extracted jar file
cmd_run = """\
IB_HOME=%(IB_HOME)s
cd $IB_HOME/IBJts
java -cp jts.jar:total.2013.jar -Xmx512M -XX:MaxPermSize=128M jclient.LoginFrame .
"""

@shared_task
def install():
    cmd = cmd_install % {'IB_HOME': IB_HOME}
    os.system(cmd)

@shared_task
def run():
    cmd = cmd_run % {'IB_HOME': IB_HOME}
    os.system(cmd)
