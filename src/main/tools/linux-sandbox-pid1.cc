// Copyright 2016 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * This is PID 1 inside the sandbox environment and runs in a separate user,
 * mount, UTS, IPC and PID namespace.
 */

#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <math.h>
#include <mntent.h>
#include <net/if.h>
#include <pwd.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/prctl.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <string>

#include "src/main/tools/linux-sandbox-options.h"
#include "src/main/tools/linux-sandbox-utils.h"
#include "src/main/tools/linux-sandbox.h"
#include "src/main/tools/process-tools.h"

static int global_child_pid;

static void SetupSelfDestruction(int *sync_pipe) {
  // We could also poll() on the pipe fd to find out when the parent goes away,
  // and rely on SIGCHLD interrupting that otherwise. That might require us to
  // install some trivial handler for SIGCHLD. Using O_ASYNC to turn the pipe
  // close into SIGIO may also work. Another option is signalfd, although that's
  // almost as obscure as this prctl.
  if (prctl(PR_SET_PDEATHSIG, SIGKILL) < 0) {
    DIE("prctl");
  }

  // Verify that the parent still lives.
  char buf = 0;
  if (close(sync_pipe[0]) < 0) {
    DIE("close");
  }
  if (write(sync_pipe[1], &buf, 1) < 0) {
    DIE("write");
  }
  if (close(sync_pipe[1]) < 0) {
    DIE("close");
  }
}

static void SetupMountNamespace() {
  // Fully isolate our mount namespace private from outside events, so that
  // mounts in the outside environment do not affect our sandbox.
  if (mount(nullptr, "/", nullptr, MS_REC | MS_PRIVATE, nullptr) < 0) {
    DIE("mount");
  }
}

static void SetupUserNamespace() {
  // Disable needs for CAP_SETGID.
  struct stat sb;
  if (stat("/proc/self/setgroups", &sb) == 0) {
    WriteFile("/proc/self/setgroups", "deny");
  } else {
    // Ignore ENOENT, because older Linux versions do not have this file (but
    // also do not require writing to it).
    if (errno != ENOENT) {
      DIE("stat(/proc/self/setgroups");
    }
  }

  int inner_uid, inner_gid;
  if (opt.fake_root) {
    // Change our username to 'root'.
    inner_uid = 0;
    inner_gid = 0;
  } else if (opt.fake_username) {
    // Change our username to 'nobody'.
    struct passwd *pwd = getpwnam("nobody");
    if (pwd == nullptr) {
      DIE("unable to find passwd entry for user nobody")
    }

    inner_uid = pwd->pw_uid;
    inner_gid = pwd->pw_gid;
  } else {
    // Do not change the username inside the sandbox.
    inner_uid = global_outer_uid;
    inner_gid = global_outer_gid;
  }

  WriteFile("/proc/self/uid_map", "%d %d 1\n", inner_uid, global_outer_uid);
  WriteFile("/proc/self/gid_map", "%d %d 1\n", inner_gid, global_outer_gid);
}

static void SetupUtsNamespace() {
  if (sethostname("localhost", 9) < 0) {
    DIE("sethostname");
  }

  if (setdomainname("localdomain", 11) < 0) {
    DIE("setdomainname");
  }
}

static void MountFilesystems() {
  for (const std::string &tmpfs_dir : opt.tmpfs_dirs) {
    PRINT_DEBUG("tmpfs: %s", tmpfs_dir.c_str());
    if (mount("tmpfs", tmpfs_dir.c_str(), "tmpfs",
              MS_NOSUID | MS_NODEV | MS_NOATIME, nullptr) < 0) {
      DIE("mount(tmpfs, %s, tmpfs, MS_NOSUID | MS_NODEV | MS_NOATIME, nullptr)",
          tmpfs_dir.c_str());
    }
  }

  // Make sure that our working directory is a mount point. The easiest way to
  // do this is by bind-mounting it upon itself.
  PRINT_DEBUG("working dir: %s", opt.working_dir.c_str());

  if (mount(opt.working_dir.c_str(), opt.working_dir.c_str(), nullptr, MS_BIND,
            nullptr) < 0) {
    DIE("mount(%s, %s, nullptr, MS_BIND, nullptr)", opt.working_dir.c_str(),
        opt.working_dir.c_str());
  }

  for (size_t i = 0; i < opt.bind_mount_sources.size(); i++) {
    std::string source = opt.bind_mount_sources.at(i);
    std::string target = opt.bind_mount_targets.at(i);
    PRINT_DEBUG("bind mount: %s -> %s", source.c_str(), target.c_str());
    if (mount(source.c_str(), target.c_str(), nullptr, MS_BIND, nullptr) < 0) {
      DIE("mount(%s, %s, nullptr, MS_BIND, nullptr)", source.c_str(),
          target.c_str());
    }
  }

  for (const std::string &writable_file : opt.writable_files) {
    PRINT_DEBUG("writable: %s", writable_file.c_str());
    if (mount(writable_file.c_str(), writable_file.c_str(), nullptr, MS_BIND,
              nullptr) < 0) {
      DIE("mount(%s, %s, nullptr, MS_BIND, nullptr)", writable_file.c_str(),
          writable_file.c_str());
    }
  }
}

// We later remount everything read-only, except the paths for which this method
// returns true.
static bool ShouldBeWritable(const std::string &mnt_dir) {
  if (mnt_dir == opt.working_dir) {
    return true;
  }

  for (const std::string &writable_file : opt.writable_files) {
    if (mnt_dir == writable_file) {
      return true;
    }
  }

  for (const std::string &tmpfs_dir : opt.tmpfs_dirs) {
    if (mnt_dir == tmpfs_dir) {
      return true;
    }
  }

  return false;
}

// Makes the whole filesystem read-only, except for the paths for which
// ShouldBeWritable returns true.
static void MakeFilesystemMostlyReadOnly() {
  FILE *mounts = setmntent("/proc/self/mounts", "r");
  if (mounts == nullptr) {
    DIE("setmntent");
  }

  struct mntent *ent;
  while ((ent = getmntent(mounts)) != nullptr) {
    int mountFlags = MS_BIND | MS_REMOUNT;

    // MS_REMOUNT does not allow us to change certain flags. This means, we have
    // to first read them out and then pass them in back again. There seems to
    // be no better way than this (an API for just getting the mount flags of a
    // mount entry as a bitmask would be great).
    if (hasmntopt(ent, "nodev") != nullptr) {
      mountFlags |= MS_NODEV;
    }
    if (hasmntopt(ent, "noexec") != nullptr) {
      mountFlags |= MS_NOEXEC;
    }
    if (hasmntopt(ent, "nosuid") != nullptr) {
      mountFlags |= MS_NOSUID;
    }
    if (hasmntopt(ent, "noatime") != nullptr) {
      mountFlags |= MS_NOATIME;
    }
    if (hasmntopt(ent, "nodiratime") != nullptr) {
      mountFlags |= MS_NODIRATIME;
    }
    if (hasmntopt(ent, "relatime") != nullptr) {
      mountFlags |= MS_RELATIME;
    }

    if (!ShouldBeWritable(ent->mnt_dir)) {
      mountFlags |= MS_RDONLY;
    }

    PRINT_DEBUG("remount %s: %s", (mountFlags & MS_RDONLY) ? "ro" : "rw",
                ent->mnt_dir);
    if (mount(nullptr, ent->mnt_dir, nullptr, mountFlags, nullptr) < 0) {
      // If we get EACCES or EPERM, this might be a mount-point for which we
      // don't have read access. Not much we can do about this, but it also
      // won't do any harm, so let's go on. The same goes for EINVAL or ENOENT,
      // which are fired in case a later mount overlaps an earlier mount, e.g.
      // consider the case of /proc, /proc/sys/fs/binfmt_misc and /proc, with
      // the latter /proc being the one that an outer sandbox has mounted on
      // top of its parent /proc. In that case, we're not allowed to remount
      // /proc/sys/fs/binfmt_misc, because it is hidden. If we get ESTALE, the
      // mount is a broken NFS mount. In the ideal case, the user would either
      // fix or remove that mount, but in cases where that's not possible, we
      // should just ignore it.
      if (errno != EACCES && errno != EPERM && errno != EINVAL &&
          errno != ENOENT && errno != ESTALE) {
        DIE("remount(nullptr, %s, nullptr, %d, nullptr)", ent->mnt_dir,
            mountFlags);
      }
    }
  }

  endmntent(mounts);
}

static void MountProc() {
  // Mount a new proc on top of the old one, because the old one still refers to
  // our parent PID namespace.
  if (mount("/proc", "/proc", "proc", MS_NODEV | MS_NOEXEC | MS_NOSUID,
            nullptr) < 0) {
    DIE("mount");
  }
}

static void SetupNetworking() {
  // When running in a separate network namespace, enable the loopback interface
  // because some application may want to use it.
  if (opt.create_netns) {
    int fd;
    fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
      DIE("socket");
    }

    struct ifreq ifr;
    memset(&ifr, 0, sizeof(ifr));
    strncpy(ifr.ifr_name, "lo", IF_NAMESIZE);

    // Verify that name is valid.
    if (if_nametoindex(ifr.ifr_name) == 0) {
      DIE("if_nametoindex");
    }

    // Enable the interface.
    ifr.ifr_flags |= IFF_UP;
    if (ioctl(fd, SIOCSIFFLAGS, &ifr) < 0) {
      DIE("ioctl");
    }

    if (close(fd) < 0) {
      DIE("close");
    }
  }
}

static void EnterSandbox() {
  if (chdir(opt.working_dir.c_str()) < 0) {
    DIE("chdir(%s)", opt.working_dir.c_str());
  }
}

static void ForwardSignal(int signum) {
  PRINT_DEBUG("ForwardSignal(%d)", signum);
  kill(-global_child_pid, signum);
}

static void SetupSignalHandlers() {
  RestoreSignalHandlersAndMask();

  for (int signum = 1; signum < NSIG; signum++) {
    switch (signum) {
      // Some signals should indeed kill us and not be forwarded to the child,
      // thus we can use the default handler.
      case SIGABRT:
      case SIGBUS:
      case SIGFPE:
      case SIGILL:
      case SIGSEGV:
      case SIGSYS:
      case SIGTRAP:
        break;
      // It's fine to use the default handler for SIGCHLD, because we use
      // waitpid() in the main loop to wait for children to die anyway.
      case SIGCHLD:
        break;
      // One does not simply install a signal handler for these two signals
      case SIGKILL:
      case SIGSTOP:
        break;
      // Ignore SIGTTIN and SIGTTOU, as we hand off the terminal to the child in
      // SpawnChild().
      case SIGTTIN:
      case SIGTTOU:
        IgnoreSignal(signum);
        break;
      // All other signals should be forwarded to the child.
      default:
        InstallSignalHandler(signum, ForwardSignal);
        break;
    }
  }
}

int Pid1Main(void *sync_pipe_param) {
  if (getpid() != 1) {
    DIE("Using PID namespaces, but we are not PID 1");
  }

  SetupSelfDestruction(reinterpret_cast<int *>(sync_pipe_param));
  SetupMountNamespace();
  SetupUserNamespace();
  if (opt.fake_hostname) {
    SetupUtsNamespace();
  }
  MountFilesystems();
  MakeFilesystemMostlyReadOnly();
  MountProc();
  SetupNetworking();
  EnterSandbox();
  SetupSignalHandlers();
  global_child_pid = SpawnCommand(opt.args);
  return WaitForChild(global_child_pid);
}