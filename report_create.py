#!/bin/python
import os
import getpass
import bugzilla
import itertools
import threading
import time
from progressbar.progressbar import ProgressBar
import yaml
from collections import OrderedDict
from operator import attrgetter
from gspread import authorize, utils
from oauth2client.service_account import ServiceAccountCredentials
from Queue import Queue
from Queue import Empty



class Bug(object):
    MAPPING = OrderedDict()
    MAPPING["ID"] = attrgetter("id")
    MAPPING["QA CONTACT"] = attrgetter("qa_contact")
    MAPPING["Summary"] = attrgetter("summary")
    MAPPING["QE test coverage"] = attrgetter("flag")
    MAPPING["Automation"] = attrgetter("automation_flag")
    MAPPING["PM score"] = attrgetter("cf_pm_score")
    MAPPING["Status"] = attrgetter("status")
    MAPPING["QA Whiteboard"] = attrgetter("qa_whiteboard")
    MAPPING["Priority"] = attrgetter("priority")
    MAPPING["Priority Index"] = attrgetter("priority_index")

    def __init__(self, **kwargs):
        for k, v in kwargs:
            setattr(self, k, v)

    @classmethod
    def create_from_bug(cls, bug):

        kwargs = {fld: attrgetter(Bug.MAPPING[fld](bug)) for fld in Bug.MAPPING.keys()}
        kwargs["bug"] = bug

        return cls(**kwargs)

    @property
    def URL(self):
        return "https://bugzilla.redhat.com/show_bug.cgi?id={bz_id}".format(bz_id=self.id)

    @property
    def automation_flag(self):
        if self.flag != "qe_test_coverage+":
            return ""

        print "getting automate_bug state"
        flag = filter(lambda f: f["name"] == "automate_bug", bug.flags)
        state = None
        if not flag:
            # bug.b.updateflags({"automate_bug": " ?"})
            state = "?"
        else:
            state = flag.pop()["status"]

        return """="{s}" """.format(s=state)

    @property
    def priority_index(self):
        index = {"high":3, "low": 1, "medium": 2, "unspecified": 0, "urgent": 4}

        return getattr({"high":3, "low": 1, "medium": 2, "unspecified": 0, "urgent": 4}, bug.priority, -1)

    def get_bug_row(self, cols_order):
        pass

class task(object):
    def __init__(self, args, kwargs):
        self.args = args
        self.kwargs = kwargs
        self.trycount = 0

class parallelizer(object):
    """
    Run parallel tasks based on the producer consumer idea.
    """

    def __init__(self, workers_limit=3, max_reties=3):
        """
        Init method for the parallelizer class
        :param workers_limit: number of workers
        :param max_reties: number of retries for failed tasks
        """

        self.max_reties = max_reties
        self.limit = workers_limit
        self.wait_for_tasks = True
        self.tasks = Queue()
        self.workers = []
        self.added_task_count = 0
        self.done_tasks = 0

    @property
    def count_of_tasks_on_the_pipeline(self):
        return self.tasks.unfinished_tasks

    @property
    def has_tasks_on_pipeline(self):
        return not self.tasks.empty()

    @property
    def get_done_percentage(self):
        return float(self.done_tasks) / self.added_task_count * 100

    def worker(self):
        """
        This function implements a sungle worker that runs tasks
        The worker manages runs his tasks using threads
        Each worker manages one thread at the time
        :return: None
        """

        while self.wait_for_tasks:
            try:
                task = self.tasks.get(timeout=1)
            except Empty as e:
                continue

            t = threading.Thread(*task.args, **task.kwargs)
            try:
                t.start()
                t.join()
            except Exception as e:
                print e.message
                print "This is try #{trycount}".format(trycount=task.trycount)

                if task.trycount <= self.limit:
                    print "Add the task for another retry"
                    self.add_for_retry(task)
            finally:
                self.tasks.task_done()
                self.done_tasks += 1

    def _add_worker(self):
        """
        Private function for add worker to the parallelizer
        :return: None
        """

        w = threading.Thread(target=self.worker)
        w.daemon = True
        self.workers.append(w)
        w.start()

    def start_parallelizer(self):
        """
        Create workers as required on init and run them.
        :return:
        """

        for _ in xrange(self.limit):
            self._add_worker()

    def stop_parallelizer(self, wait_for_workers_cleanup=True, wait_for_tasks_in_queue=True):
        """
        Stop the parallelizer
        :param join: wait for unfinished tasks
        :return: None
        """

        # wait until the queue become empty if required
        while self.has_tasks_on_pipeline and wait_for_tasks_in_queue:
            print "has tasks: {}".format(self.has_tasks_on_pipeline)
            print "wait? : {}".format(wait_for_tasks_in_queue)
            time.sleep(0.1)


        self.wait_for_tasks = False

        if wait_for_workers_cleanup:
            map(lambda t: t.join(), self.workers)

    def add_task(self, *args, **kwargs):
        """
        "Add a new task to do"
        :param args: args to pass the execution thread
        :param kwargs: kwargs to pass the execution thread
        :return: None
        """

        self.tasks.put(task(args, kwargs))
        self.added_task_count += 1

    def add_for_retry(self, task):
        """
        Add a task in retry mode
        :param task: task to add
        :return:
        """

        task.trycount += 1
        self.tasks.put(task)

class report_gen(object):

    def __init__(self, url, username, password):
        self.bz = bugzilla.Bugzilla(url=url)
        self.bz.bug_autorefresh = True
        self.bz.login(user=username, password=password)
        self.parallelizer = parallelizer(5)
        self.all_bugs = []

    def get_issues_for_qa_contact(self, contact, flag):
        query = {'bug_status': ['NEW', 'ASSIGNED', 'POST', 'MODIFIED', 'ON_DEV', 'ON_QA', 'VERIFIED', 'RELEASE_PENDING', 'CLOSED'],
         'email1': '{contact}'.format(contact="|".join(contact if isinstance(contact, list) else [contact])),
         'emailqa_contact1': '1',
         'emailtype1': 'regexp',
         'f1': 'flagtypes.name',
         'list_id': '8703795',
         'o1': 'allwordssubstr',
         'query_format': 'advanced',
         'v1': "{flag}".format(flag=flag)}

        result = self.bz.query(query)
        for bug in result:
            self.all_bugs.append(self.get_bug_row(bug, flag))

    def get_report_title(self):
        return "BZ, QA CONTACT, Summary, Status, Flag, Automate, Priority, Priority Index"

    def resolve_header(self, bug, flag, filed):

        def get_automated(bug, flag):

            if flag != "qe_test_coverage+":
                return ""

            flag = filter(lambda f: f["name"] == "automate_bug", bug.flags)
            state = None
            if not flag:
                state = "?"
            else:
                state = flag.pop()["status"]

            return """="{s}" """ .format(s=state)

        def get_info(bug, flag):
            kwargs = {"target": get_automated, "args": (bug, flag)}
            self.parallelizer.add_task(**kwargs)

        mapping = {"BZ": lambda bug, _: "=hyperlink(\"https://bugzilla.redhat.com/show_bug.cgi?id={bz_id}\",\"{bz_id}\")".format(bz_id=bug.id),
                   "QA CONTACT": lambda bug, _: bug.qa_contact,
                   "Summary": lambda bug, _: " ".join(bug.summary.split(",")),
                   "Flag": lambda _, flag: flag,
                   "Automate": get_info,
                   "PM score": lambda bug, _: bug.cf_pm_score,
                   "URL": lambda bug, _: "https://bugzilla.redhat.com/show_bug.cgi?id={bz_id}".format(bz_id=bug.id),
                   "Status": lambda bug, _: bug.status,
                   "Reporter": lambda bug, _: bug.reporter,
                   "QA Whiteboard": lambda bug, _: bug.qa_whiteboard,
                   "Priority": lambda bug, _: bug.priority,
                   "Priority Index": lambda bug, _: {"high": 3,
                                                     "low": 1,
                                                     "medium": 2,
                                                     "unspecified": 0,
                                                     "urgent": 4}.get(bug.priority, -1)}

        fld = filed.replace('\n', "").lstrip()
        return mapping[fld](bug, flag)

    def bug_to_string(self, bug, flag):
        result_string = ""
        for filed in self.get_report_title().split(","):
            result_string = "{old}{comma}{new}".format(old=result_string, comma=", " if len(result_string) > 0 else "",
                                                       new=self.resolve_header(bug, flag, filed))

        return result_string

    def get_bug_row(self, bug, flag):
        return [self.resolve_header(bug, flag, filed) for filed in self.get_report_title().split(",")]

    def get_filed_col_index(self, filed):
        return utils.rowcol_to_a1(1,
                                  [s.lstrip() for s in self.get_report_title().replace('\n', "").split(",")].index(filed) + 1)[:1]

    def save_to_file(self, path):

        with open(path, "w") as f:
            f.write(self.get_report_title())
            for bug in self.all_bugs:
                f.write("{line}\n".format(line=",".join(bug)))

    def save_to_google_drive_full_report(self, contacts, flags, certificat_name):

        REPORT_FILE_NAME = "QE Test coverage report"

        def get_full_report_request():
            kwargs = dict()
            kwargs["range"] = "Full Report!A1:{last_cell}".format(
                last_cell=utils.rowcol_to_a1(len(self.all_bugs) + 1, len(self.get_report_title().split(","))))
            kwargs["params"] = {"valueInputOption": "USER_ENTERED"}

            payload = dict()
            payload["majorDimension"] = "ROWS"
            payload["values"] = list(
                itertools.chain(*[[self.get_report_title().split(",")], [bug for bug in self.all_bugs]]))

            kwargs["body"] = payload
            return kwargs

        def get_summery_request():
            payload = dict()
            kwargs = dict()
            kwargs["range"] = "Summary!A1:{last_cell}".format(
                last_cell=utils.rowcol_to_a1(len(contacts) + 2, len(flags) + 2))
            kwargs["params"] = {"valueInputOption": "USER_ENTERED"}

            payload["majorDimension"] = "ROWS"
            values = list()
            values.append(list(itertools.chain(*[[" "], flags])))

            for contact in contacts:
                v = list()
                v.append(contact)
                for flag in flags:
                    v.append("=COUNTIFS('Full Report'!{c}2:{c}1000, \"{contact}\",'Full Report'!{f}2:{f}1000,\"{flag}\")".format(
                        contact=contact, flag=flag.replace("?","~?"),
                        c=self.get_filed_col_index("QA CONTACT"),
                        f=self.get_filed_col_index("Flag")))
                values.append(v)
            payload["values"] = values
            kwargs["body"] = payload
            return kwargs

        def get_sort_request(spreadsheet):
            sheetId = filter(lambda w: w.title == "Full Report", spreadsheet.worksheets()).pop().id
            payload = {"requests": [
                {
                    "sortRange": {
                    "range": {
                        "sheetId": sheetId,
                        "startRowIndex": 1,
                        "endRowIndex": 1000,
                        "startColumnIndex": 0,
                        "endColumnIndex": 8},
                    "sortSpecs": [
                        {
                            "dimensionIndex":
                                [f.rstrip().lstrip() for f in self.get_report_title().split(",")].index("Priority"),
                            "sortOrder": "DESCENDING"
                        }
                    ]
                    }
                }
            ]
            }
            return {"body": payload}

        scope = ['https://spreadsheets.google.com/feeds', "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(certificat_name, scope)

        client = authorize(creds)

        spreadsheet = None
        if REPORT_FILE_NAME not in [curr_spreadsheet["name"] for curr_spreadsheet in client.list_spreadsheet_files()]:
            spreadsheet = client.create(REPORT_FILE_NAME)
            spreadsheet.add_worksheet("Full Report", 1000, 1000)
            spreadsheet.add_worksheet("Summary", 1000, 1000)
            spreadsheet.share("snaim@redhat.com", perm_type="user", role="writer")
        else:
            spreadsheet = client.open(REPORT_FILE_NAME)

        spreadsheet.values_update(**get_full_report_request())
        spreadsheet.values_update(**get_summery_request())
        spreadsheet.batch_update(**get_sort_request(spreadsheet))

        client.session.close()

def main():

    print "Loading configuration"
    with open("config.cfg", "r") as f:
        config = yaml.load(f)

    qa_contacts = config["qa_contacts"]
    flags = config["flags"]
    username = config.get("username", None)
    password = config.get("password", None)
    certificate_name = config["certificate_name"]

    username = os.getenv("BZ_USER", username)
    if not username:
        print ("Username was neither available at the config file or env vars (var name: BZ_USER)\n"
              "please input user name manually")
        username = raw_input("Username: ")

    password = os.getenv("BZ_PASSWORD", password)
    if not password:
        print ("Password was neither available at the config file or env vars (BZ_USER)\n"
               "please input password manually")
        password = getpass.getpass()

    print "Collecting issues from Bugzilla"
    report_builder = report_gen("https://bugzilla.redhat.com", username=username, password=password)
    report_builder.parallelizer.start_parallelizer()

    for flag in flags:
        report_builder.get_issues_for_qa_contact(qa_contacts, flag)

    print ""
    print "*********************"
    print ""
    print "*** Total ***********"
    print "{count} issues found".format(count=len(report_builder.all_bugs))
    print "***By User **********"

    issues_by_user = {}

    for issue in report_builder.all_bugs:
        contact = issue[1]
        issues_by_user[contact] = issues_by_user.get(contact,0) + 1

    for contact, count in issues_by_user.items():
        print "{contact}: {count}".format(contact=contact, count=count)
    print "*********************"
    print ""

    pbar = ProgressBar().start()
    pbar.start()

    while report_builder.parallelizer.has_tasks_on_pipeline:
        pbar.update(report_builder.parallelizer.get_done_percentage)
        time.sleep(0.2)
    pbar.finish()

    print "Waiting for all the workers to finish"
    report_builder.parallelizer.stop_parallelizer()

    print "Saving report to G cloud"
    report_builder.save_to_google_drive_full_report(qa_contacts, flags, certificate_name)

if __name__ == "__main__":
    main()
