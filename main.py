#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
from __future__ import print_function

'''HDP Package Manager (hpm)
@organization: Daimler Financial Services
@author: Dirk Voss
@contact: dirk.voss@daimler.com
@change: 2017-08-31, creation, Dirk Voss
'''

################################################################################
# MODULE
#    hpm.main
# DESCRIPTION
#    HDP Package Manager (HPM)
#    Main module to run from a command line
# ASSUMPTIONS/PRECONDITIONS
#    /
# KNOWN ISSUES
#    - setting file system modes is not working correct when using
#      the parameter in os.makedirs
# TODO: write all outputs parallel to a log file
# TODO: user module config_git to track changes in config files
# TODO: remove package
# TODO: set group and owner
# TODO: implement dry-run (like "--noop")
################################################################################

import optparse
import platform
import sys
import os
import shutil
import pwd
import subprocess
import datetime
import time
import re
import zipfile

################################################################################
# Configuration
################################################################################

from hpm.config import MODE_READ, MODE_WRITE, MODE_EXEC, \
                       MODE_DIR_OWNER_READ, MODE_DIR_OWNER_WRITE, \
                       MODE_DIR_GROUP_WRITE, MODE_DIR_ALL_WRITE

from hpm.config import CLUSTER_SERVER_PREFIX, CLUSTER_NAME

from hpm.config import TENANTS, TENANT_SCRIPT, DATA_CENTER

from hpm.config_git import Config, VcsError

from hpm.module_info import ModuleInfo, DepGraph


################################################################################
# Classes
################################################################################

class HpmError (Exception):
    pass

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# CLASS Logging
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Logfile(object):

    def __init__(self, params):
        """Create log file"""
        self.path = self._create(params)

    def append(self, lines):
        try:
            with open(self.path, "a") as f:
                for line in lines:
                    print(line, file=f)
        except IOError:
            raise HpmError("Log file '" + logfile + "' could not be appended!")

    def _create (self, params):
        # set installation log directory path
        logdir = os.path.join(params.root_path, "data", "SYS", "share", "log")
        logfile = os.path.join(logdir, "hpm_" + params.timestamp + ".log")
        try:
            with open(logfile, "a") as f:
                print("TIME     :",
                    params.starttime.strftime("%Y-%m-%d %H:%M:%S.%f "+time.strftime("%Z")),
                    file=f)
                print("USER     :", params.proc_user, file=f)
                print("SUDO_USER:", params.sudo_user, file=f)
                print("COMMAND  :", params.command, file=f)
                print("CALL     :", " ".join(sys.argv), file=f)
                print("FROM_DIR :", params.start_dir, file=f)
                print(80*"-", file=f)
        except IOError:
            raise HpmError("Log file '" + logfile + "' could not be written!")
        return logfile


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# CLASS Parameters
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Parameters(object):

    def __init__(self, argv, start_dir=None):
        """Checks and stores command line options"""
        # parse command line call and store arguments and options
        self._parser = self._get_parser()
        (options, args) = self._parser.parse_args(argv[1:])
        if len(args) < 1:
            self._parser.print_usage()
            raise HpmError("Incorrect number of arguments")
        # command
        self.command = args[0].lower()
        # arguments
        self.args = args[1:]
        # directory the comand was started from
        if start_dir:
            self.start_dir = start_dir
        else:
            self.start_dir = os.getcwd()
        # options
        self.options = options
        self.tenant = options.tenant
        self.data_center = options.data_center
        self.verbose = options.verbose
        self.quiet = options.quiet
        self.overwrite = options.overwrite
        self.user = options.user
        self.proc_user = pwd.getpwuid(os.getuid())[0]
        self.sudo_user = os.getenv("SUDO_USER")
        self.root_path = os.path.abspath(options.root_path)
        self.cluster = options.cluster
        self.show_all = options.show_all
        self.starttime = datetime.datetime.now()
        self.tzname = time.strftime("%Z")
        self.timestamp = self.starttime.strftime("%Y%m%d_%H%M%S_%f_"+time.strftime("%Z"))

    def print_usage(self):
        self._parser.print_usage()

    def print_help(self):
        self._parser.print_help()

    def _get_parser(self):
        # define command line
        parser = optparse.OptionParser(usage=USAGE)
        parser.add_option("-t", "--tenant", dest="tenant",
                          help="one of " + str(Tenant.get_tenant_names()))
        parser.add_option("-d", "--data-center", dest="data_center",
                          help="one of ['" + ", '".join(DATA_CENTER.keys()) + "']")
        parser.add_option("-a", "--show-all",
                          action="store_true", dest="show_all",
                          help="show all available information")
        parser.add_option("-v", "--verbose",
                          action="store_true", dest="verbose",
                          help="verbose execution")
        parser.add_option("-q", "--quiet",
                          action="store_true", dest="quiet",
                          help="quiet execution")
        # options to enforce derivation of parameters or overwrite
        parser.add_option("--force-overwrite",
                          action="store_true", dest="overwrite",
                          help="overwrite installed version")
        parser.add_option("--force-user",
                          action="store", dest="user",
                          help="set name of service user")
        parser.add_option("--force-root-path",
                          action="store", dest="root_path", default="/",
                          help="set root path of installation")
        cluster_names = list(CLUSTER_SERVER_PREFIX.values())
        cluster_names.sort()
        parser.add_option("--force-cluster",
                          action="store", dest="cluster",
                          help="set cluster as one of " + str(cluster_names))
        # parse arguments
        return parser


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# CLASS Package
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Package(object):

    def __init__(self, path):
        if not os.path.isfile(path):
            raise HpmError("package file '" + path + "' not found or accessible")
        self.path = path
        # get the information out of the zip-file member not its file name
        foldername = Package.extract_folder_from_zip(self.path)
        self.project, self.application, self.module, self.version \
            = Package.parse_foldername(foldername)
        self.name = \
           self.project + "_" \
         + self.application + "__" \
         + self.module
        self.dirname = self.name + "_" + self.version
        self.jarfile = self.dirname + ".jar"

    @staticmethod
    def extract_folder_from_zip(path):
        """Returns the name of the folder of the first element in a zip file"""
        try:
            zip_file = zipfile.ZipFile(path, 'r')
            folder = zip_file.namelist()[0].split("/")[0]
        except:
            raise HpmError("file '" + path + "' is not a valid package zip file")
        return folder

    @staticmethod
    def parse_foldername(foldername):
        """Infers the package definition from the package name"""
        pat_project = r"[a-z][a-z0-9\-]+"
        pat_application = r"[a-z][a-z0-9\-]+"
        pat_module = r"[a-z][a-z0-9\-]+"
        pat_version = r"\d+\.\d+\.\d+[A-Za-z0-9\-]*"
        pattern = r"^" \
                + r"(" + pat_project + r")" + r"_" \
                + r"(" + pat_application + r")" + r"__" \
                + r"(" + pat_module + r")" + r"_" \
                + r"(" + pat_version + r")"
        match = re.search(pattern, foldername)
        if not match:
            print("\n  BAD PACKAGE NAME!")
            print("\n    Package name (folder extracted from zip file) given:")
            print("      " + foldername)
            print("    The package name is defined as")
            print("      <project>_<application>__<module>_<version>")
            print("    and has to match the following regular expression:")
            print("      " + pattern)
            raise HpmError("Package name does not match to an HPM package!")
        project = match.group(1)
        application = match.group(2)
        module = match.group(3)
        version = match.group(4)
        # validate module
        module_type = module.split("-")[0]
        if module_type not in ["app", "inf", "lib", "mon", "srv", "tst", "utl"]:
            print("\n  BAD MODULE NAME!")
            print("\n    Module name (folder extracted from zip file) given:")
            print("      " + module)
            print("    The module name is defined as")
            print("        <type>-<name>")
            print("    where <type> is one of the following:")
            print("      app, inf, lib, mon, srv, tst, utl")
            raise HpmError("Package module name does not match to an HPM package!")
        return (project, application, module, version)
        # validate version
        pattern = r"^\d+\.\d+\.\d+$|^\d+\.\d+\.\d+\-[A-Za-z0-9\-]+$"
        match = re.search(pattern, version)
        if not match:
            print("\n  BAD VERSION!")
            print("\n    Version (folder extracted from zip file) given:")
            print("      " + version)
            print("    The version is defined as")
            print("        <major>.<minor>.<maintenance>[-<label>]")
            print("    and has to match the following regular expression:")
            print("      " + pattern)
            raise HpmError("Package version name does not match to an HPM package!")
        return (project, application, module, version)

    def show(self, prefix=""):
        print(prefix+"SOURCE")
        print(prefix+"  File       : " + self.path)
        print(prefix+"PACKAGE")
        print(prefix+"  Name       : " + self.name)
        print(prefix+"  Project    : " + self.project)
        print(prefix+"  Application: " + self.application)
        if self.module:
            print(prefix+"  Module     : " + self.module)
        else:
            print(prefix+"  Module     : " + "-")
        print(prefix+"  Version    : " + self.version)

#TODO: move the following to Installation
    def get_application_dir(self, tenant):
        return os.path.join(tenant.path, self.project, self.application)

    def get_module_dir(self, tenant):
        return os.path.join(tenant.path, self.project, self.application,
                            self.module)

    def get_target_dir(self, tenant):
        return os.path.join(tenant.path, self.project, self.application,
                            self.module, self.version)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# CLASS Installation
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Installation(object):

    def __init__(self, tenant, package):
        self.tenant = tenant
        self.package = package

    def get_versions(self):
        """Get installed versions and current version"""
        module_path = self.package.get_module_dir(self.tenant)
        if not os.path.isdir(module_path):
            return ([], None)
        versions = []
        current = None
        for fn in os.listdir(module_path):
            version_dir = os.path.join(module_path,fn)
            if os.path.islink(version_dir):
                if fn == "current":
                    current = os.path.realpath(version_dir)
                    current = os.path.split(current)[-1]
                else:
                    pass # unknown file
            elif os.path.isdir(version_dir):
                if fn[:8] != "removed_":
                    versions.append(fn)
            else:
                pass # unknown file
        return (versions, current)

    def show_versions(self, prefix=""):
        """List installed versions and current version"""
        (versions, current) = self.get_versions()
        print("VERSIONS")
        if versions:
            print(prefix+"  Installed  : "+", ".join(versions))
            if current:
                print(prefix+"  Current    : "+current)
            else:
                print(prefix+"  Current    : -")
        else:
            print(prefix+"  no installations found")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# CLASS Tenant
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Tenant(object):

    def __init__(self, tenant, cluster=None, data_center=None, root_path="/"):
        tenants = Tenant.get_tenant_names()
        if tenant not in tenants:
            raise HpmError("Unknown tenant, i.e. not one of " \
                           + str(tenants))
        if not cluster:
            (cluster, _) = Tenant.derive_cluster()
        if not data_center:
            (_, data_center) = Tenant.derive_cluster()
        self.cluster = cluster
        self.data_center = data_center
        self.name = tenant
        if tenant != "SYS":
            # other tenants than SYS are assigned to a cluster
            cluster = TENANTS[tenant]["cluster"]
            if cluster != self.cluster.split("-")[0]: # DEV|DEV-DR -> DEV
                raise HpmError("Tenant '" +  self.name \
                              + "' does not fit to cluster '" + cluster + "'!")
        self.port_offset = TENANTS[tenant]["port_offset"]
        self.user_prefix = TENANTS[tenant]["user_prefix"]
        self.path = os.path.join(root_path,"data",tenant)
        if not os.path.isdir(self.path):
            if root_path == "/":
                script_args = self.name + " " + os.sep
            else:
                script_args = self.name + " " + root_path
            raise HpmError("Tenant '" + self.name + "' does not exist! Info with\n" \
                         + "  # " + TENANT_SCRIPT + " " + script_args)

    def show(self, prefix="", show_all=False):
        print(prefix + "CLUSTER")
        print(prefix + "  Cluster: " + self.cluster)
        if os.path.isdir(self.path):
            print(prefix + "TENANT")
            print(prefix + "  Name   : " + self.name)
            print(prefix + "  Path   : " + self.path)
            module_versions = self.get_module_versions()
            if len(module_versions) == 0:
                print(prefix + "  Modules: None")
            elif show_all:
                print(prefix + "  Modules: All versions")
            else:
                print(prefix + "  Modules: Current version")
            for (prj,app,mod,ver,is_current) in module_versions:
                if show_all:
                    if is_current:
                        current = " <- active version"
                    else:
                        current = ""
                    print(prefix + "    %-10s %-10s %-20s %s%s" % (prj, app, mod, ver, current))
                else:
                    if is_current:
                        print(prefix + "    %-10s %-10s %-20s %s" % (prj, app, mod, ver))
        else:
            raise HpmError("Tenant not found ('" + self.path + "'!")

    def list_modules(self, show_all=False):
        module_versions = self.get_module_versions()
        for (prj,app,mod,ver,is_current) in module_versions:
            if is_current:
                current = "current"
            else:
                current = "old"
            if show_all  or is_current:
                print(";".join([self.cluster, self.name, prj, app, mod, ver, current]))

    def get_module_versions (self):
        """Returns a list of all installed module versions"""
        module_versions = []
        project_folders = [ folder for folder in os.listdir(self.path)
                                if folder not in ["share"] ]
        project_folders.sort()
        for project in project_folders:
            project_path = os.path.join(self.path, project)
            if not os.path.isdir(project_path): continue
            app_folders = os.listdir(project_path)
            app_folders.sort()
            for app in app_folders:
                app_path = os.path.join(project_path, app)
                if not os.path.isdir(app_path): continue
                module_folders = os.listdir(app_path)
                module_folders.sort()
                for module in module_folders:
                    module_path = os.path.join(app_path, module)
                    if not os.path.isdir(module_path): continue
                    version_folders = [ folder for folder in os.listdir(module_path)
                                            if folder not in ["current"] ]
                    version_folders.sort()
                    current_version = os.path.realpath(os.path.join(module_path, "current")) \
                                        .split(os.sep)[-1]
                    for version in version_folders:
                        version_path = os.path.join(module_path, version)
                        if not os.path.isdir(version_path): continue
                        if version[:8] != "removed_":
                            module_versions.append((project, app, module, version,
                                                    version == current_version))
        return module_versions

    @staticmethod
    def get_tenant_names():
        """Return list of accepted tenant names"""
        tenants = list(TENANTS.keys())
        tenants.sort()
        return tenants

    @staticmethod
    def derive_tenant(path, name, root_path="/", cluster=None, data_center=None):
        """Derive tenant from string or - if not set - from package path"""
        if not name:
            try:
                # tenant from MFT directory:
                #   /data/mft/deployment/e032_hdp_<tenant>/process/<package>
                folders =  os.path.split(path)[0] \
                                  .split(os.path.sep)
                if folders[-3] != "deployment":
                    raise HpmError("Tenant could not be derived from package path" \
                            + " -> please use command line option")
                name = "_".join(folders[-2].split("_")[-2:])
            except IndexError:
                raise HpmError("Tenant could not be derived from package path" \
                            + " -> please use command line option")
        return Tenant(name, cluster, data_center, root_path)

    @staticmethod
    def derive_cluster(node=None):
        """Derive data center and cluster name from node name"""
        if not node:
            node = platform.node()
        prefix_to_cluster = {}
        for (cluster, prefix) in CLUSTER_SERVER_PREFIX.items():
            prefix_to_cluster[prefix] = cluster
        cluster = prefix_to_cluster.get(node[0], None)
        if not cluster:
            #TODO: raise HpmError("Unknown node '" + node + "'")
            # everything else is DEV
            cluster = "DEV"
        # derive data center
        data_center = node[9] # depuasdhb220 => depuasdhb 2 20
        if  data_center in ["1", "2"]:
            # mark disaster recovery cluster
            if cluster == "DEV" and data_center == "1":
                cluster = "DEV-DR"
            elif cluster == "INT" and data_center == "1":
                cluster = "INT-DR"
            elif cluster == "PROD" and data_center == "2":
                cluster = "PROD-DR"
        else:
            data_center = "unknown"
        return (cluster, data_center)

    @staticmethod
    def find_tenants(root_path="/"):
        """Find tenants found in file system"""
        data_dir = os.path.join(root_path,"data")
        if os.path.isdir(data_dir):
            subfolders = os.listdir(data_dir)
            tenants = [folder for folder in subfolders
                               if folder in TENANTS.keys()]
        else:
            raise HpmError("No base directory '/data' found!")
        return tenants

    @staticmethod
    def show_missing_tenants(cluster, tenants_found, root_path="/", prefix=""):
        """Show tenants missing in current cluster"""
        missing_tenants = []
        for tenant_name in TENANTS.keys():
            if TENANTS[tenant_name]["cluster"] == cluster:
                if tenant_name not in tenants_found:
                    missing_tenants.append(tenant_name)
        if "SYS" not in tenants_found:
            missing_tenants.append("SYS")
        if len(missing_tenants) > 0:
            missing_tenants.sort()
            print(prefix + "MISSING TENANTS")
            print(prefix + "  " + str(missing_tenants))
            print(prefix + "MISSING TENANTS COULD BE CREATED BY")
            if root_path == "/":
                script_args = " " + os.sep
            else:
                script_args = " " + root_path
            for tenant_name in missing_tenants:
                print(prefix + "  # " + TENANT_SCRIPT + " " + tenant_name \
                      + script_args)

    @staticmethod
    def show_cluster(root_path="/", prefix=""):
        """Prints information about the cluster"""
        node = platform.node()
        (cluster, data_center) = Tenant.derive_cluster()
        print(prefix + "CURRENT NODE")
        print(prefix + "  Node       : " + node)
        print(prefix + "CLUSTER")
        print(prefix + "  Name       : " + cluster)
        print(prefix + "  ID         : " + CLUSTER_NAME[cluster])
        print(prefix + "  Data Center: " + data_center)
        print(prefix + "TENANTS")
        tenants = Tenant.find_tenants(root_path)
        tenants.sort()
        if tenants:
            for tenant in tenants:
                print(prefix + "  " + tenant)
        else:
            if root_path == "/":
                basedir = "/data"
            else:
                basedir = os.path.join(root_path,"data")
            print(prefix + "  No tenants found, i.e no folder" \
                  + " '" + basedir + os.sep + "<tenant>'!")
        Tenant.show_missing_tenants(cluster, tenants, root_path, prefix)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# CLASS User
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class User(object):

    def __init__(self, tenant, project, application, user_name=None):
        if user_name:
            # use name provided
            self.name = user_name
            self.keytab = ""
            self.principal = ""
        else:
            self.name = TENANTS[tenant.name]["user_prefix"]
            cluster_server_prefix = "d" # default
            for k,v in CLUSTER_SERVER_PREFIX.items():
                if k == tenant.cluster:
                    cluster_server_prefix = v
            self.name = self.name.replace("${CLUSER_SERVER_PREFIX}",
                                          cluster_server_prefix)
            print("TENANT",tenant.name)
            print("DC",tenant.data_center)
            print("NAME",self.name)
            if project == "sys":
                # only system tools => no keytab file provided
                self.keytab = ""
                self.principal = ""
            else:
                sep = self.name[-1] # use same separator, e.g. im_de_e_dp, s-de-e-dp
                self.name = self.name + project[0] + sep + application
                # since the old and new data centers use different characters
                # in names, the separator "_" has to be exchanged by an allowed char
                self.keytab = "/etc/security/keytabs/e032_s_" \
                    + self.name + ".keytab"
                self.principal = "E032_S_"+self.name.upper()+"@EMEA.CORPDS.NET"
            if not self.name:
                raise HpmError("Could not determine service user.")


################################################################################
# Tasks
################################################################################

def get_and_show_package(path, tenant_name, data_center, root_path="/"):
    tenant = Tenant.derive_tenant(path, tenant_name, data_center=data_center, root_path=root_path)
    print("TENANT\n  " + tenant.name)
    pkg = Package(path)
    pkg.show()
    inst = Installation(tenant, pkg)
    inst.show_versions()
    print("TARGET")
    print("  "+pkg.get_target_dir(tenant))
    return (pkg, inst)

def check_package(package):
    # check if zip is OK
    # # unzip -t bad1.zip 1> /dev/null 2>&1 ; echo $?
    # check if main folder in zip is OK
    print("CHECK "+package.name)

def unpack_package(package, tenant, target_path=None):
    if target_path:
        # extract to non standard path (e.g. for tests)
        module_path = target_path
    else:
        module_path = package.get_module_dir(tenant)
    install_path = os.path.join(module_path, package.version)
    extract_path = os.path.join(module_path, "prep")
    if os.path.isdir(extract_path):
        shutil.rmtree(extract_path)
    os.makedirs(extract_path)
    os.chmod(extract_path,MODE_DIR_OWNER_WRITE)
    rc = subprocess.call(["unzip","-qq",package.path,"-d",extract_path])
    if rc != 0:
        shutil.rmtree(extract_path)
        raise HpmError("error extracting package file")
    # test, if the extracted content is OK
    if os.listdir(extract_path) != [package.dirname]:
        shutil.rmtree(extract_path)
        raise HpmError("package has to unzip to the package directory '" \
                      + package.dirname + "' and only to this")
    # move to right folder and rename
    src = os.path.join(extract_path, package.dirname)
    dst = install_path
    os.rename(src,dst)
    os.rmdir(extract_path)
    os.chmod(install_path,MODE_DIR_OWNER_WRITE)
    # create/empty log directory
    module_logdir = os.path.join(install_path, "log")
    if os.path.isdir(module_logdir):
        shutil.rmtree(module_logdir)
    os.makedirs(module_logdir,MODE_DIR_OWNER_WRITE)
    # update permissions on all elements
    for root, dirs, files in os.walk(dst, topdown=False):
        for name in files:
            file_path = os.path.join(root, name)
            if os.path.splitext(name)[1] == ".sh":
                # executable files: *.sh
                os.chmod(file_path,MODE_EXEC)
            elif (os.path.basename(root) == "conf") and (os.path.splitext(name)[1] != ".template"):
                # configuration files: conf/* except *.template
                os.chmod(file_path,MODE_WRITE)
            else:
                # all other files
                os.chmod(file_path,MODE_READ)
        for name in dirs:
            dir_path = os.path.join(root, name)
            if name == "log":
                # log directories: log
                os.chmod(dir_path,MODE_DIR_OWNER_WRITE)
            elif name == "conf":
                # log directories: log
                os.chmod(dir_path,MODE_DIR_OWNER_WRITE)
            else:
                # all other directories
                os.chmod(dir_path,MODE_DIR_OWNER_WRITE)
    #TODO: set owner and group
    print("  Package extracted to\n  "+module_path)

def create_env_file(package, tenant, user, logdir, root_path="/"):
    # >>> env_file = "tenant-env.sh.template"
    # >>> assignment_pattern = r"export [A-Z_]*=.*"
    # >>> assignments = [ l.strip() for l in open(env_file,"r")
    #                     if re.match(assignment_pattern,l) is not None ]
    # >>> env_vars = [ a.split("=")[0][7:] for a in assignments ]
    # check is set => split to (key, value)
    #   r = re.match(r"^export ([A-Z_]+)=([^#\s].+)","export A=\"b\" # Hallo \"OK\"")
    #   (r.group(1),r.group(2)) if r
    # command "check-env" or with info/install/update?
    # conf
    #   cluster-env.sh.template
    #   PRJ_APP__MOD-env.sh.template
    #   tenant-env.sh.template
    # ebip_dp__el-cds/1.0/src/main/resources/conf
    #   mod__ebip_dp__el_cds-env.sh.template
    install_path = package.get_target_dir(tenant)
    config_dir = os.path.join(install_path, "conf")
    env_file = package.name + "-env.sh"
    env_path = os.path.join(config_dir, env_file)
    print("  Create tenant file\n    "+env_path)
    if not os.path.isdir(config_dir):
        os.makedirs(config_dir,MODE_DIR_OWNER_WRITE)
    env_file = open(env_path,"w")
    print("#!/usr/bin/env bash", file=env_file)
    conf_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "..", "..", "conf")
    conf_template = os.path.join(os.path.abspath(conf_dir), "PRJ_APP__MOD-env.sh.template")
    template= [ line.rstrip() for line in open(conf_template,"r") ]
    for line in template:
        placeholders = re.findall(r"(\{\{[A-Z_]+\}\})", line)
        stripped_root_path = root_path
        if stripped_root_path[-1] == "/":
             stripped_root_path = root_path[:-1]
        prj_app = package.project.upper() + "_" + package.application.upper()
        if len(placeholders)>0:
            line = line \
              .replace(r"{{ROOT_PATH}}", stripped_root_path) \
              .replace(r"{{TENANT}}", tenant.name) \
              .replace(r"{{PROJECT}}", package.project) \
              .replace(r"{{APPLICATION}}", package.application) \
              .replace(r"{{MODULE}}", package.module) \
              .replace(r"{{VERSION}}", package.version) \
              .replace(r"{{JARNAME}}", package.jarfile) \
              .replace(r"{{APPUSER}}", "$MBBHDP_" + prj_app + "_USER") \
              .replace(r"{{APPKEYTAB}}", "$MBBHDP_" + prj_app + "_KEYTAB") \
              .replace(r"{{APPPRINCIPAL}}", "$MBBHDP_" + prj_app + "_PRINCIPAL") \
              .replace(r"{{LOGDIR}}", install_path+os.sep+"log") \
              .replace(r"{{INSTLOGDIR}}", logdir)
            not_replaced = re.findall(r"(\{\{[A-Z_]+\}\})", line)
            if len(not_replaced)>0:
                raise HpmError("Unkown placeholder(s) " + str(not_replaced))
        print(line, file=env_file)
    print("", file=env_file)
    env_file.close()
    return env_path

def versioningConfigFiles(tenant):
    """Commit all changes of the given tenant to the git repository"""
    tenant_config = Config(tenant.path) # uses tenant_path + /share/conf
    tenant_config.show()
    config_path = os.path.join(tenant.path, "share", "conf")
    config_files = [ fn for fn in os.listdir(config_path)
                        if os.path.splitext(fn)[1] != ".bak" ]
    tenant_config.documentOtherUpdates(config_files)


################################################################################
# Help
################################################################################

USAGE = \
"""hpm <command> [options] [arguments]
    <command> is one of the following:
        help         usage information
        info         information about the cluster, tenant or package
        install      install a package
        readme       show basic documentation of a package
        remove       remove a package
        update       update a package
        check-info   checks a MODULE.yml file
        display-info  display dependencies defined in a MODULE.yml file
    For additional information type 'hpm help [command]'."""

HELP_INFO = \
"""Usage: hpm info <object> [arguments]
    <object> is one of the following terms:
        cluster  (default)
        tenant   a tenant name has to be given as an argument
        modules
        package  a package path has to be given as an argument
    The parameter --show-all could be used to show all modules.
    Examples:
        hpm info
        hpm info tenant DEVPI_DE
        hpm info modules
        hpm info package /tmp/ebip_dp__dummy_0.1.0.zip"""

HELP_INSTALL = \
"""Usage: hpm install <package> [options]"""

HELP_README = \
"""Usage: hpm readme <package> [options]"""

HELP_REMOVE = \
"""Usage: hpm remove <package-name> [options]"""

HELP_UPDATE = \
"""Usage: hpm update <package> [options]"""

HELP_CHECK_INFO = \
"""Usage: hpm check-info MODULEFILE [options]"""

HELP_DISPLAY_INFO = \
"""Usage: hpm display-info MODULEFILE [options]
    Displays dependency diagrams from the information given in the MODULE.yml file."""


################################################################################
# COMMANDS
################################################################################

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# command help
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def cmd_help(params):
    """Print command line help"""
    if len(params.args) == 0:
        params.print_help()
    else:
        command = params.args[0]
        if command == "info":
            print(HELP_INFO)
        elif command == "install":
            print(HELP_INSTALL)
        elif command == "readme":
            print(HELP_README)
        elif command == "remove":
            print(HELP_REMOVE)
        elif command == "update":
            print(HELP_UPDATE)
        elif command == "check-info":
            print(HELP_CHECK_INFO)
        else:
            params.print_usage()
            raise HpmError("Unknown command '" + command + "'")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# command readme
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def cmd_readme(params):
    """Open INSTALL.txt for reading"""
    if len(params.args) != 1:
        params.print_usage()
        raise HpmError("Argument PACKAGE is missing")
    path = params.args[0]
    cmd = "\"\"unzip -qq -c "+path+" '*/INSTALL.txt' | less\"\""
    rc = subprocess.call(["sh","-c",cmd])
    if rc != 0:
        raise HpmError("error extracting INSTALL.txt")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# command info
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def cmd_info(params):
    """Print information about the installations"""
    if len(params.args) == 0:
        obj = "cluster"
    else:
        obj = params.args[0]
    if obj == "cluster":
        Tenant.show_cluster(params.root_path)
    elif obj == "tenant":
        if len(params.args) == 2:
            tenant_name = params.args[1]
            tenant = Tenant(tenant_name, cluster=params.cluster,
                            data_center=params.data_center,
                            root_path=params.root_path)
            tenant.show(show_all=params.show_all)
        else:
            raise HpmError("Argument TENANT is missing")
    elif obj == "modules":
        for tenant_name in Tenant.find_tenants(params.root_path):
            tenant = Tenant(tenant_name, cluster=params.cluster,
                            data_center=params.data_center,
                            root_path=params.root_path)
            tenant.list_modules(show_all=params.show_all)
    elif obj == "package":
        if len(params.args) == 2:
            path = params.args[1]
            # get and print basic information
            (pkg, inst) = get_and_show_package(path, \
                                       params.tenant, \
                                       params.data_center, \
                                       params.root_path)
            tenant = inst.tenant
            user = User(tenant, pkg.project, pkg.application, params.user)
            print(user.name)
            print("TODO: Info about the package", path)
        else:
            raise HpmError("Argument PACKAGE is missing")
    else:
        print(HELP_INFO)
        raise HpmError("Unknown information object")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# command install and update
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def run_script(script, config_file, help_msg="", working_dir=None):
    """Executes a script"""
    print("EXECUTE "+script)
    script_path = os.path.abspath(script)
    config_path = os.path.abspath(config_file)
    if working_dir:
        # switch to working directory
        start_dir = os.getcwd()
        os.chdir(working_dir)
    #TODO catch errors
    print("-"*80)
    sys.stdout.flush()
    sys.stderr.flush()
    try:
        rc = subprocess.call([script_path, config_path])
    except Exception as err:
        raise HpmError("CALL: " + script_path + " " + config_path)
    sys.stdout.flush()
    sys.stderr.flush()
    print("-"*80)
    if working_dir:
        # switch back
        os.chdir(start_dir)
    if rc != 0:
        raise HpmError("Script '" + script + "' failed" + help_msg)
    print("  OK")

def check_user(package, tenant, user, user_name=None):
    """Checks and returns the user"""
    # derive user from installation path
    app_dir = package.get_application_dir(tenant)
    uid = os.stat(app_dir).st_uid
    name = pwd.getpwuid(os.stat(app_dir).st_uid).pw_name
    # user_name overwrites
    if not user_name and name != user.name:
        raise HpmError("Owner of application directory is not '" \
                        + user.name + "'")
    # check if user fits
    if os.getuid() != uid:
        raise HpmError("Script has to be run as service user" \
                     + " owning the application folder, i.e. "
                     +"'"+name+"'")
    return name

def cmd_install(params, update=False):
    """Installs or updates a module"""
    if len(params.args) != 1:
        params.print_usage()
        raise HpmError("Argument PACKAGE is missing")
    path = params.args[0]
    # create log file
    logfile = Logfile(params)
    # get and print basic information
    (pkg, inst) = get_and_show_package(path, \
                                       params.tenant, \
                                       params.data_center, \
                                       params.root_path)
    tenant = inst.tenant
    # check if application folder exists
    application_dir = pkg.get_application_dir(tenant)
    if not os.path.isdir(application_dir):
        raise HpmError("Application directory '" + application_dir \
                     + "' has to be created before an installation")
    # user
    user = User(tenant, pkg.project, pkg.application, params.user)
    check_user(pkg, tenant, user, params.user)
    print("SERVICE USER (owner of installation):\n  "+user.name)
    # paths
    module_path = pkg.get_module_dir(tenant)
    install_path = os.path.join(module_path, pkg.version)
    # set installation log directory path
    logdir = os.path.join(tenant.path, "share", "log",
                pkg.project + "_" + pkg.application,
                params.timestamp + "__" + pkg.module + "_" + pkg.version)
    # log file for package scripts
    if not os.path.isdir(logdir):
        os.makedirs(logdir)
        # set mode in makedirs does not work correctly, set it in a second step
        os.chmod(logdir,MODE_DIR_GROUP_WRITE)
    if not os.path.isdir(os.path.dirname(logdir)):
        os.chmod(os.path.dirname(logdir),MODE_DIR_GROUP_WRITE)
    # check if version is already installed
    (versions, _) = inst.get_versions()
    if (not update):
        if len(versions)>1 or (len(versions)==1 and pkg.version not in versions):
            if not params.overwrite:
                # other versions installed
                raise HpmError("Package module is already installed!" \
                             + " Use command update if that was intended.")
    if pkg.version in versions:
        if params.overwrite:
            print("EXISTING VERSION IS REMOVED (folder renamed to 'removed_*_TIMESTAMP')")
            remove_path = os.path.join(module_path, "removed_" + pkg.version + "_" \
                                       + params.timestamp)
            os.rename(install_path, remove_path)
            #TODO maybe with --purge: shutil.rmtree(install_path):
        else:
            raise HpmError("Package version already installed" \
                        + " Use option '--force-overwrite' to replace.")
    #TODO: Move versioning to a separate action
#   # before installation versioning existing config files
#   try:
#       versioningConfigFiles(tenant)
#   except VcsError as error:
#       print("WARNING: " + str(error))
    # start installation ...
    if update:
        task="update"
        print("UPDATE")
    else:
        task="install"
        print("INSTALL")
    print("  HPM log file: " + logfile.path)
    print("  Package log files: " + logdir)

    print("\n1. Test package")
    print("  OK")

    print("\n2. Extract zip and rename folder to version")
    unpack_package(pkg,tenant)
    print("  OK")

    print("\n3. Check content")
    module_file = os.path.join(install_path, "MODULE.yml")
    print("3.1 Mandatory Files")
    for filename in ["MODULE.yml", "CHANGELOG.md", "README.md", "INSTALL.md"]:
        if not os.path.exists(os.path.join(install_path, filename)):
            raise HpmError("File '" + filename + "' is missing in module directory!")
    print("  OK")
    print("3.2 MODULE.yml")
    print("File:", module_file)
    print(40*"-")
    module_info = ModuleInfo(module_file)
    module_info.show()
    print(40*"-")
    print("  OK")

    print("\n4. Create config file <name>-env.sh")
    config_file = create_env_file(pkg,tenant, user, logdir, params.root_path)
    print("  OK")

    print("\n5. run pre-" + task + " script - stop services (optional)")
    script = os.path.join(install_path, "install", "pre_" + task + ".sh")
    if os.path.isfile(script):
        run_script(script, config_file, working_dir=application_dir)
    else:
        print("  no script provided by the package")

    print("\n6. run " + task + " script - configure (optional)")
    script = os.path.join(install_path, "install", task + ".sh")
    if os.path.isfile(script):
        run_script(script, config_file,
            "\n  Fix errors and reinstall with '--force-overwrite'",
            working_dir=application_dir)
    else:
        print("  no script provided by the package")

    print("\n7. switch current version to package version")
    current_link = os.path.join(module_path, "current")
    if os.path.islink(current_link):
        os.remove(current_link)
    os.symlink(pkg.version,current_link)

    print("\n8. run post-" + task + " script - start services (optional)")
    script = os.path.join(install_path, "install", "post_" + task + ".sh")
    if os.path.isfile(script):
        run_script(script, config_file, working_dir=application_dir)
    else:
        print("  no script provided by the package")

    # Print status
    print("")
    print("#"*40)
    print("# Package successfully installed       #")
    print("#"*40)
    # append status to the logfile
    logfile.append(["RESULT   : " + "Package successfully installed"])


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# command remove
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def cmd_remove(params):
    """Remove module version"""
    if len(params.args) != 1:
        params.print_usage()
        raise HpmError("Argument PACKAGE is missing")
    path = params.args[0]
    # create log file
    logfile = Logfile(params)
    # get and print basic information
    tenant = Tenant.derive_tenant(path, params.tenant, params.data_center, root_path=params.root_path)
    (project, application, module, version) =  Package.parse_filename(path)
    print("Remove:",project, application, module, version)
    # check if module version exists
    installed = tenant.get_module_versions()
    version_current = [ (ver,is_current) for (prj, app, mod, ver, is_current) in installed
                            if prj == project and app == application and mod == module ]
    versions = [ ver for (ver,is_current) in version_current ]
    current_version = [ ver for (ver,is_current) in version_current if is_current ][0]
    print("installed:", versions)
    print("current:", current_version)
    if version not in versions:
        raise HpmError("Version to be removed (" + version + ") is not installed!")
    if len(versions) == 1:
        raise HpmError("Last installed version could not be removed!")
    if version == current_version:
        raise HpmError("Remove of current version (" + version + ") not implemented!")
    else:
        timestamp = "".join([ "%02d"%t for t in \
                      datetime.datetime.now().timetuple()[:6] ])
        module_path = os.path.join(tenant.path, project, application, module)
        install_path = os.path.join(module_path, version)
        remove_path = os.path.join(module_path, "removed_" + version + "_" + timestamp)
        os.rename(install_path, remove_path)
        print("EXISTING VERSION '" + version + "'REMOVED (folder renamed to 'removed_*_TIMESTAMP')")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# command check-info
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def cmd_check_info(params):
    """Checks MODULE.yml files"""
    if len(params.args) != 1:
        params.print_usage()
        raise HpmError("Argument MODULEFILE is missing")
    module_file = params.args[0]
    module_info = ModuleInfo(module_file)
    module_info.show()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# command display-info
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def cmd_display_info(params):
    """Display MODULE.yml files"""
    if len(params.args) != 1:
        params.print_usage()
        raise HpmError("Argument MODULEFILE is missing")
    module_file = params.args[0]
    module_info = ModuleInfo(module_file)
    graph = DepGraph(module_info)
    graph.display()


################################################################################
# MAIN
################################################################################

def main(argv):
    """Main"""
    params = Parameters(argv)
    # execute command
    if params.command == "help":
        cmd_help(params)
    elif params.command == "info":
        cmd_info(params)
    elif params.command == "install":
        cmd_install(params, update=False)
    elif params.command == "readme":
        cmd_readme(params)
    elif params.command == "remove":
        cmd_remove(params)
    elif params.command == "update":
        cmd_install(params, update=True)
    elif params.command == "check-info":
        cmd_check_info(params)
    elif params.command == "display-info":
        cmd_display_info(params)
    else:
        params.print_usage()
        raise HpmError("COMMAND '" + params.command + "' unknown")


################################################################################

if __name__ == "__main__":
    if not __debug__:
        sys.tracebacklimit = 0
    # prohibit running with root rights
    if os.getuid() == 0:
        print("hpm: [ERROR] " \
              + "Script is not allowed to be executed by root!")
        exit(1)
    # since the script could be called via sudo, access to the current
    # directory could fail
    try:
        os.chdir(os.getcwd())
    except OSError:
        username = pwd.getpwuid(os.getuid())[0]
        print("hpm: [ERROR] Please run script from a directory accessible by '" \
              + username + "'", file=sys.stderr)
        sys.exit(1)
    # print stack dumps if debug option is set
    if __debug__:
        # full stack dump
        main(sys.argv)
    else:
        # only error messages
        try:
            main(sys.argv)
        except HpmError as error:
            print("hpm: [ERROR] " + str(error), file=sys.stderr)
            sys.exit(1)
    exit(0)


