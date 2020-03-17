#!/usr/bin/env python 
#
# Cloudlet Infrastructure for Mobile Computing
#
#   Author: Kiryong Ha <krha@cmu.edu>
#
#   Copyright (C) 2011-2013 Carnegie Mellon University
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

"""
DB wrapper for cloudlet
"""

import os
import sqlalchemy
import sys
from ..configuration import Const
from sqlalchemy.orm import sessionmaker
import datetime

import table_def


class DBConnector(object):
    def __init__(self, log=sys.stdout):

        # create DB file if it does not exist
        if not os.path.exists(Const.CLOUDLET_DB):
            log.write("[DB] Create new database\n")
            dirpath = os.path.dirname(Const.CLOUDLET_DB)
            if os.path.exists(dirpath) == False:
                os.makedirs(dirpath)
            table_def.create_db(Const.CLOUDLET_DB)

        # mapping existing DB to class
        self.engine = sqlalchemy.create_engine('sqlite:///%s' % Const.CLOUDLET_DB, echo=False)
        session_maker = sessionmaker(bind=self.engine)
        self.session = session_maker()

    def add_item(self, entry):
        self.session.add(entry)
        self.session.commit()

    def del_item(self, entry):
        self.session.delete(entry)
        self.session.commit()

    def update_item(self, entry):
        self.session.update(entry)
        self.session.commit()

    def list_item(self, entry):
        ret = self.session.query(entry)
        return ret


