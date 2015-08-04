#-*-coding:utf-8-*-

import os
import sys
import types
import json
import time
import mylog
import MySQLdb

def daemon():
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError,e:
        sys.stderr.write('fork #2 failed: %d (%s)\n' % (e.errno, e.strerror))
        sys.exit(1)
    
    os.setsid()
    os.umask(0) 
    
    try:
        pid = os.fork()
        if pid > 0:
            print "Daemon PID %d" % pid
            sys.exit(0)
    except OSError,e:
        sys.stderr.write('fork #2 failed: %d (%s)\n' % (e.errno, e.strerror))
        sys.exit(1)

def reconn(config):
    return MySQLdb.connect(config['db_conn']['host'],
        config['db_conn']['user'],
        config['db_conn']['passwd'],
        config['db_conn']['db'],
        charset=config['db_conn']['charset'])

'''
    load_config
'''
def load_config(fname):
    f = open(fname)
    cfg = json.loads(f.read())
    f.close()
    return cfg

'''
    load_service
'''
def load_service(fp):
    r = {}
    sys.path.append(fp)
    for _,_,filenames in os.walk(fp):
        for filename in filenames:
            basename = os.path.splitext(filename)[0]
            sufix = os.path.splitext(filename)[1][1:]
            if sufix == 'py':
                if not basename in sys.modules:
                    r[basename] = __import__(basename,globals(),locals())
    return r

'''
    get instance
'''
def get_svc_obj(smap,mname,cname):
    if mname in smap:
        cls = getattr(smap[mname],cname)
        return cls()
    else:
        return None

if __name__ == '__main__':

    daemon()
    
    config = load_config('./etc/simple_task.cfg')

    svc_map = load_service('./service')

    svc_list = []
    conn = None

    try:
        conn = MySQLdb.connect(config['db_conn']['host'],
            config['db_conn']['user'],
            config['db_conn']['passwd'],
            config['db_conn']['db'],
            charset=config['db_conn']['charset'])

        cur = conn.cursor()
        cur.execute("SELECT module_name,cls_name,sche_type,date_format(sche_at,'%H:%i:%s'),priority FROM t_service WHERE `status` = 1")
        svc_list = cur.fetchall()

        cur.close()
        conn.commit();
    except Exception,e:
        print e
        conn.close()
        sys.exit(1)

    while True:
        try:
            if conn.ping() == False:
                conn = reconn(config)

            h,m = time.localtime(time.time())[3],time.localtime(time.time())[4]

            for svc in svc_list:
                obj = get_svc_obj(svc_map,svc[0],svc[1])
                if obj:
                    if svc[2] == 'rt':
                        obj.handle(conn)
                    else:
                        stime = time.strptime(svc[3],"%H:%M:%S")
                        shour,smin = stime[3],stime[4]
                        if shour == h and smin == m:
                            obj.handle(conn)
        except Exception,e:
            mylog.error(e)

        time.sleep(60)
