#encoding=utf-8
import cx_Oracle
import time
import sched
import os
from cStringIO  import StringIO
from sqlalchemy import create_engine

from app.common.core_api import  add_image,remove_image
from app.foundation import db,log
from app.common.exceptions import NetworkError, CoreError,DBError
from app.common.view_helper import save_stream
from app.models.Subject import Subject
from app.models.Group import Group
from app.models.Photo import Photo
from app.models.Core import Core
from app.models.Update_time import Update_time

SPECIFY_UPDATE_TIME_AFTER  = 17
db_username = 'root'
db_password = 'root'
db_ip = '20.3.2.18'
db_port = 3306
db_name = 'security_ss'
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
DB_LINK = 'tjgx_rxk/dragon@192.168.20.81:11522/ORCL'
# 'viot/viot@10.101.6.93:1521/WXGARXCS'
query = 'SELECT * FROM TJGX_RXK.V_ALL_IMAGE'
# 'select * from VIOT.T_ALL_IMAGE'
schedule = sched.scheduler(time.time,time.sleep)

GROUP_NAME_DICT = {
    '0' : '常住人口',
    '1' : '全国在逃',
    '2' : '全国违法犯罪',
    '3' : '流动人口',
    '4' : '驾驶人员信息',
    '5' : '铁路',
    '6' : '网吧',
    '7' : '民航',
    '8' : '旅馆',
    '9' : '看守所',
    '10': '实名盾',
    '11': '嫌疑人'
    }


def conn_mysql():
    new_engine = create_engine("mysql://%s:%s@%s:%s/%s"%(db_username,db_password,db_ip,db_port,db_name))
    new_session = new_engine.connect()
    return new_session


def __convert_xb(sfzh):
    xb = u'未知'
    try:
        xb_num = 0
        if 18 == len(sfzh):
            xb_num = int(sfzh[16])
        else:
            xb_num = int(sfzh[13])
        if(xb_num % 2 == 0):
            xb = u'女'
        else:
            xb = u'男'
    except Exception as e:
        print e.message
    return xb

#分别得到需要增加和删除的group_id列表
def data_from_to_group_id(data_from,row):
    groups_to_add = []
    groups_to_del = []
    new_session = conn_mysql()
    subjects_ = new_session.execute('select * from subject where cert_id = {}'.format(row[0]))
    subject = subjects_.fetchone()
    # subject = Subject.query.filter(Subject.cert_id == row[0]).first()
    if subject:
        while  subject:
            if not subject.remark:
                for index, flag in enumerate(data_from):
                    if flag == '1':
                        groups_to_add.append(GROUP_NAME_DICT[str(index)])
            elif subject.remark != data_from and subject.delete == 0:
                for index, flag in enumerate(data_from):
                    for index_, flag_ in enumerate(subject.remark):
                        if index == index_ and flag != flag_:
                            if flag == '1':
                                groups_to_add.append(GROUP_NAME_DICT[str(index)])
                            elif flag == '0':
                                groups_to_del.append(GROUP_NAME_DICT[str(index)])
                        # if flag != flag_ and flag == '1':
                        #     groups_to_add.append(GROUP_NAME_DICT[str(index)])
                        # elif flag != flag_ and flag == '0' and index == index_:
                        #     groups_to_del.append(GROUP_NAME_DICT[str(index)])
            try:
                subject = subjects_.fetchone()
            except:
                pass
    else:
        for index, flag in enumerate(data_from):
            if flag == '1':
                groups_to_add.append(GROUP_NAME_DICT[str(index)])
    new_session.close()
    gender_ = __convert_xb(row[0])
    group_ids_to_add = []
    group_ids_to_del = []
    if groups_to_add:
        groups_to_add = list(set(groups_to_add))
        group_ids_to_add = get_group_id(groups_to_add,gender_)
    if groups_to_del:
        groups_to_del = list(set(groups_to_del))
        group_ids_to_del = get_group_id(groups_to_del,gender_)
    return group_ids_to_add,group_ids_to_del

def get_group_id(groups, gender_, group_ids = None):
    if group_ids == None:
        group_ids = []
    for category in groups:
        group_id= Group.query.filter_by(category = category,gender = gender_).first().id
        group_ids.append(group_id)
    return group_ids

def incremental_update(inc):
    hour_now = time.strftime('%H', time.localtime(time.time()))
    if int(hour_now) >= SPECIFY_UPDATE_TIME_AFTER :
        db_conn = cx_Oracle.connect(DB_LINK)
        orc_cursor = db_conn.cursor()
        orc_cursor.execute(query)
        row = orc_cursor.fetchone()
        while row:
            data_from, insert_time = row[8], row[10] #data_from 是一个长度为256的字符串，例：'01000101010'
            group_ids_to_add,group_ids_to_del = data_from_to_group_id(data_from,row) #根据oracle的照片来源判断group_id
            if group_ids_to_add:
                update_subject(group_ids_to_add, row,add_subject, insert_time,data_from)
            if group_ids_to_del:
                update_subject(group_ids_to_del, row,del_subject,insert_time,data_from)
            try:
                row = orc_cursor.fetchone()
            except :
                pass
    log.info('All is well')
    schedule.enter(inc, 0, incremental_update, (inc,))

def update_subject(group_ids, row,update_subject_, insert_time,data_from_):
    for data_from in group_ids:
        group = Group.query.filter(Group.id == data_from).first() 
        update_time = Update_time.query.filter(Update_time.group_id == group.id).first()
        #根据update_time有无数据来判断是否为首次更新
        if update_time: 
            # if str(insert_time) > update_time.update_time:
            result = update_subject_(row,group,data_from_)
            update_time.update_time = str(insert_time)
            try:
                db.session.commit()
            except Exception as e:
                db.session.rollback()
                raise DBError(message=e.message)
        else:
            log.info('{}{} 首次进行增量更新'.format(group.category.encode('utf-8'),group.gender.encode('utf-8')))
            result = add_subject(row,group,data_from_)
            update_time = Update_time(
                group_id = group.id,
                update_time = insert_time
            )
            db.session.add(update_time)
            try:
                db.session.commit()
            except Exception as e:
                db.session.rollback()
                raise DBError(message=e.message)

def add_subject(row,group,data_from):
    img_buff = StringIO(row[5].read())
    district, name, cert_id = '', row[1], row[0]
    img_uri = save_stream(img_buff)
    core_ = Core.query.filter(Core.id == group.core_id).first()
    ip,port = core_.ip,core_.port
    basic_group_name = 'basic' + str(group.id)
    try:
            ret = add_image(ip,port,basic_group_name,img_buff,'',True)
            group_index = ret['id']
    except Exception  as e:
        log.error('过core失败：{}'.format(e))
        group_index = -1

    # subject = Subject.query.filter(Subject.cert_id == cert_id).first()
    # if not subject:
    subject = Subject(
        group_id = group.id,
        category = group.category,
        district = district,
        gender = group.gender,
        name = name,
        cert_id = cert_id,
        remark = data_from,
        timestamp = time.time()
    )
    db.session.add(subject)
    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        log.error('{}存入subject表失败：{}'.format(name,e))
        return 0
    # else:
    #     subject.remark = data_from
    #     try:
    #         db.session.commit()
    #     except Exception as e:
    #         db.session.rollback()
    #         log.error('{}存入subject表失败：{}'.format(name,e))
    #         return 0

         
         
    photo = Photo(
            group_id = group.id,
            subject_id = subject.id,
            group_index = group_index,
            path = img_uri,
            rect = '',
            tag = '',
    )
    db.session.add(photo)
    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        log.error('{}存入photo表失败：{}'.format(name,e))
        return 0


def del_subject(row,group,data_from):
    district, name, cert_id = '', row[1], row[0]
    core_ = Core.query.filter(Core.id == group.core_id).first()
    ip,port = core_.ip,core_.port
    basic_group_name = 'basic' + str(group.id)
    new_session = conn_mysql()
    subject_ = new_session.execute('select * from subject where cert_id = {} and `delete` = 0'.format(cert_id))
    subjects = subject_.fetchall()
    # subjects = Subject.query.filter(Subject.cert_id == \
        # cert_id, Subject.delete == 0).all()
    for subject in subjects:
        if subject.group_id == group.id:
            subject.delete = 1
        subject.remark = data_from
        try:
            db.session.commit()
        except Exception as e:
            db.session.rollback()

        photo_ = new_session.execute('select * from photo where subject_id = {} and `delete` = 0'.format(subject.id))
        photos = photo_.fetchall()
        # photos = Photo.query.filter(Photo.subject_id == subject.id, Photo.delete == 0).all()
        for photo in photos:
            try:
                ret = remove_image(ip, port, basic_group_name, photo.group_index)
            except Exception  as e:
                log.error(e)
            photo.delete = 1
            try:
                db.session.commit()
            except Exception as e:
                db.session.rollback()


def main(inc = 60):
        schedule.enter(0,0,incremental_update,(inc,))
        schedule.run()


if __name__ == '__main__':
    main(1800)
