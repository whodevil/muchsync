
#include <cassert>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <fcntl.h>
#include <signal.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include "cleanup.h"
#include "infinibuf.h"
#include "notmuch_db.h"

using namespace std;

static unordered_set<string>
lines(const string &s)
{
  istringstream is (s);
  string line;
  unordered_set<string> ret;
  while (getline(is, line))
    ret.insert(line);
  return ret;
}

static string
chomp(string s)
{
  while (s.length() && (s.back() == '\n' || s.back() == '\r'))
    s.resize(s.length() - 1);
  return s;
}

static bool
conf_to_bool(string s)
{
  s = chomp(s);
  if (s.empty() || s == "false" || s == "0")
    return false;
  return true;
}

static char*
str_to_char_star(string s) {
  const auto ret = new char[s.length() + 1];
  strcpy(ret, s.c_str());
  return ret;
}

notmuch_db::message_t
notmuch_db::get_message(const char *msgid)
{
  notmuch_message_t *message;
  nmtry("notmuch_database_find_message",
	notmuch_database_find_message (notmuch(), msgid, &message));
  return message_t (message);
}

notmuch_db::message_t
notmuch_db::add_message(const string &path, const tags_t *newtags,
			bool *was_new)
{
  notmuch_status_t err;
  notmuch_message_t *message;
#if LIBNOTMUCH_CHECK_VERSION(5,1,0)
  err = notmuch_database_index_file(notmuch(), path.c_str(), nullptr, &message);
#else // libnotmuch < 5.1.0
  err = notmuch_database_add_message(notmuch(), path.c_str(), &message);
#endif // libnotmuch < 5.1.0
  if (err != NOTMUCH_STATUS_DUPLICATE_MESSAGE_ID) {
    nmtry("notmuch_database_add_message", err);
    set_tags(message, newtags ? *newtags : new_tags);
  }
  if (was_new)
    *was_new = err != NOTMUCH_STATUS_DUPLICATE_MESSAGE_ID;
  return message_t (message);
}

void
notmuch_db::remove_message(const string &path)
{
  notmuch_status_t err =
    notmuch_database_remove_message(notmuch(), path.c_str());
  if (err != NOTMUCH_STATUS_DUPLICATE_MESSAGE_ID)
    nmtry("notmuch_database_remove_message", err);
}

void
notmuch_db::set_tags(notmuch_message_t *msg, const tags_t &tags)
{
  // Deliberately don't unthaw message if we throw exception
  nmtry("notmuch_message_freeze", notmuch_message_freeze(msg));
  nmtry("notmuch_message_remove_all_tags",
	notmuch_message_remove_all_tags(msg));
  for (auto tag : tags)
    nmtry("notmuch_message_add_tag", notmuch_message_add_tag(msg, tag.c_str()));
  if (sync_flags)
    nmtry("notmuch_message_maildir_flags_to_tags",
	  notmuch_message_tags_to_maildir_flags(msg));
  nmtry("notmuch_message_thaw", notmuch_message_thaw(msg));
}

string
notmuch_db::default_notmuch_config()
{
  char *p = getenv("NOTMUCH_CONFIG");
  if (p && *p)
    return p;
  p = getenv("HOME");
  if (p && *p)
    return string(p) + "/.notmuch-config";
  throw runtime_error ("Cannot find HOME directory\n");
}

string
notmuch_db::get_config(const char *config, int *err)
{
  const char *av[] { notmuch_path, "config", "get", config, nullptr };
  return run_notmuch(av, nullptr, err);
}

void
notmuch_db::set_config(const char *config, ...)
{
  va_list ap;
  va_start(ap, config);
  vector<const char *> av { notmuch_path, "config", "set", config };
  const char *a;
  do {
    a = va_arg(ap, const char *);
    av.push_back(a);
  } while (a);
  run_notmuch(av.data(), "[notmuch] ");
}

notmuch_db::notmuch_db(string config, string path, bool create)
  : notmuch_config (config),
    notmuch_path (str_to_char_star(path)),
    maildir (chomp(get_config("database.path"))),
    new_tags (lines(get_config("new.tags"))),
    and_tags (make_and_tags()),
    sync_flags (conf_to_bool(get_config("maildir.synchronize_flags")))
{
  if (maildir.empty())
    throw runtime_error(notmuch_config + ": no database.path in config file");
  if (create) {
    struct stat sb;
    string nmdir = maildir + "/.notmuch";
    int err = stat(nmdir.c_str(), &sb);
    if (!err && S_ISDIR(sb.st_mode))
      return;
    if (!err || errno != ENOENT)
      throw runtime_error(nmdir + ": cannot access directory");
    mkdir(maildir.c_str(), 0777);
    nmtry("notmuch_database_create",
	  notmuch_database_create(maildir.c_str(), &notmuch_));
  }
}

notmuch_db::~notmuch_db()
{
  close();
}

notmuch_db::tags_t
notmuch_db::make_and_tags()
{
  int err;
  string s = get_config("muchsync.and_tags", &err);
  return err ? new_tags : lines(s);
}

string
notmuch_db::run_notmuch(const char *const *av, const char *errprefix,
			int *exit_value)
{
  int fds[2];
  if (pipe(fds) != 0)
    throw runtime_error (string("pipe: ") + strerror(errno));
  pid_t pid = fork();
  switch (pid) {
  case -1:
    {
      string err = string("fork: ") + strerror(errno);
      ::close(fds[0]);
      ::close(fds[1]);
      throw runtime_error (err);
    }
  case 0:
    ::close(fds[0]);
    if (errprefix && fds[1] != 2)
      dup2(fds[1], 2);
    if (fds[1] != 1) {
      dup2(fds[1], 1);
      if (errprefix && fds[1] != 2)
	::close(fds[1]);
    }
    setenv("NOTMUCH_CONFIG", notmuch_config.c_str(), 1);

    int err = -1;
    if (exit_value) {
      // Since the caller is looking at exit value, suppress chatter
      err = dup(2);
      ::close(2);
      fcntl(err, F_SETFD, 1);
      ::open("/dev/null", O_WRONLY);
    }

    execvp(notmuch_path, const_cast<char *const*> (av));

    if (err != -1)
      dup2(err, 2);

    cerr << "notmuch: " << strerror(errno) << endl;
    // Use SIGINT as hacky way to convey that exec failed
    raise(SIGINT);
    _exit(127);
  }

  ::close(fds[1]);
  ifdstream in (fds[0]);
  ostringstream os;

  if (errprefix) {
    string line;
    while (getline(in, line))
      cerr << errprefix << line << '\n';
  }
  else
    os << in.rdbuf();

  int status;
  if (waitpid(pid, &status, 0) != pid)
    assert(!"waitpid failed waiting for notmuch");
  else if (!WIFEXITED(status)) {
    if (WIFSIGNALED(status)) {
      if (WTERMSIG(status) == SIGINT)
	throw runtime_error ("could not run notmuch");
      else
	throw runtime_error ("notmuch exited with signal "
			     + std::to_string(WTERMSIG(status)));
    }
    else
      throw runtime_error ("notmuch exit status " + std::to_string(status));
  }
  if (exit_value)
    *exit_value = WEXITSTATUS(status);
  return os.str();
}

Xapian::docid
notmuch_db::get_dir_docid(const char *path)
{
  unique_obj<notmuch_directory_t, notmuch_directory_destroy> dir;
  nmtry("notmuch_database_get_directory",
	notmuch_database_get_directory(notmuch(), path, &dir.get()));
  if (!dir)
    throw range_error (path + string (": directory not found in notmuch"));

  /* XXX -- evil evil */
  struct fake_directory {
    notmuch_database_t *notmuch;
    Xapian::docid doc_id;
  };
  return reinterpret_cast<const fake_directory *>(dir.get())->doc_id;
}

notmuch_database_t *
notmuch_db::notmuch ()
{
  if (!notmuch_) {
    notmuch_status_t err =
      notmuch_database_open (maildir.c_str(),
			     NOTMUCH_DATABASE_MODE_READ_WRITE,
			     &notmuch_);
    if (err)
      throw runtime_error (maildir + ": "
			   + notmuch_status_to_string(err));
  }
  return notmuch_;
}

void
notmuch_db::close()
{
  if (notmuch_)
    notmuch_database_destroy (notmuch_);
  notmuch_ = nullptr;
}

void
notmuch_db::run_new(const char *prefix)
{
  const char *av[] = { "notmuch", "new", nullptr };
  close();
  run_notmuch(av, prefix);
}
