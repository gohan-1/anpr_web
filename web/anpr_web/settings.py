

import os
import sys
from pathlib import Path

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = BASE_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))  # lets Django import the pipeline package

# ---- Environment (local / staging / production): see anpr_web/env.py --------------
# Settings come from envs/.env.<environment> (e.g. envs/.env.staging). For local, the
# older main/.env is read as well, so an existing setup keeps working unchanged.
from dotenv import load_dotenv  # noqa: E402

from .env import env_bool, env_list, load_environment  # noqa: E402

ANPR_ENV = load_environment()
IS_LOCAL = ANPR_ENV == "local"
if IS_LOCAL:
    load_dotenv(PROJECT_ROOT / ".env", override=False)

# Read-only mode: the app refuses to change anything (no review actions, no bots, no
# migrations). Useful when a laptop points at production data just to look.
READ_ONLY = env_bool("ANPR_READ_ONLY", default=False)

# The secret key signs logins AND encrypts the bots' API passwords, so each environment
# must keep ITS key forever (a new key = bot passwords must be typed in again).
# Local falls back to the development key; staging/production must set DJANGO_SECRET_KEY.
SECRET_KEY = os.environ.get("DJANGO_SECRET_KEY") or (
    'django-insecure-i-!+pwez7g2#n1gq*+43_srz)e17!+l!&ggi#wk)d#3#up4m^o' if IS_LOCAL else None)
if not SECRET_KEY:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured(f"DJANGO_SECRET_KEY is missing in envs/.env.{ANPR_ENV}")

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = env_bool("DJANGO_DEBUG", default=IS_LOCAL)

ALLOWED_HOSTS = env_list("DJANGO_ALLOWED_HOSTS", "localhost,127.0.0.1")


# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'rest_framework',            # Django REST Framework: builds the API
    'rest_framework.authtoken',  # login tokens for the React app
    'core',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'core.middleware.ReadOnlyMiddleware',       # blocks changes when READ_ONLY is on
]

ROOT_URLCONF = 'anpr_web.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'anpr_web.wsgi.application'


# Database
# https://docs.djangoproject.com/en/6.1/ref/settings/#databases

# Which database, first match wins:
#   ANPR_DB_PATH=/tmp/x.sqlite3  -> that SQLite file (tests and the e2e sandbox)
#   ANPR_DB_ENGINE=sqlite        -> SQLite file web/db.sqlite3 (unit tests force this)
#   DATABASE_URL=postgres://USER:PASSWORD@HOST:5432/NAME   -> envs/.env.staging / production
#   ANPR_DB_ENGINE=postgres + POSTGRES_* -> PostgreSQL (the original main/.env way)
#   otherwise, on local          -> SQLite file web/db.sqlite3
import dj_database_url  # noqa: E402

SQLITE_OPTIONS = {
    "timeout": 20,                               # wait for a busy database instead of failing
    "transaction_mode": "IMMEDIATE",             # take the write lock up front: fewer "database is locked"
    "init_command": "PRAGMA journal_mode=WAL;",  # readers don't block writers
}
_engine = os.environ.get("ANPR_DB_ENGINE", "").lower()
if os.environ.get("ANPR_DB_PATH"):
    DATABASES = {"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": os.environ["ANPR_DB_PATH"],
                             "OPTIONS": SQLITE_OPTIONS}}
elif _engine == "sqlite":
    DATABASES = {"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": BASE_DIR / "db.sqlite3",
                             "OPTIONS": SQLITE_OPTIONS}}
elif os.environ.get("DATABASE_URL"):
    DATABASES = {"default": dj_database_url.parse(
        os.environ["DATABASE_URL"],
        conn_max_age=60,             # reuse connections: bots query every second
        conn_health_checks=True)}    # drop a dead connection instead of erroring
elif _engine in ("postgres", "postgresql"):
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.postgresql',
            'NAME': os.environ.get('POSTGRES_DB', 'anpr'),
            'USER': os.environ.get('POSTGRES_USER', 'anpr'),
            'PASSWORD': os.environ.get('POSTGRES_PASSWORD', ''),
            'HOST': os.environ.get('POSTGRES_HOST', 'localhost'),
            'PORT': os.environ.get('POSTGRES_PORT', '5432'),
            'CONN_MAX_AGE': 60,
            'CONN_HEALTH_CHECKS': True,
        }
    }
elif IS_LOCAL:
    DATABASES = {"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": BASE_DIR / "db.sqlite3",
                             "OPTIONS": SQLITE_OPTIONS}}
else:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured(f"DATABASE_URL is missing in envs/.env.{ANPR_ENV}")

if DATABASES["default"]["ENGINE"].endswith("sqlite3"):
    # Tests: a real file instead of SQLite's in-memory default, so tests where several
    # bots (threads) use the database at once behave like the real thing.
    import tempfile  # noqa: E402
    DATABASES["default"]["TEST"] = {"NAME": str(Path(tempfile.gettempdir()) / f"anpr_test_{os.getpid()}.sqlite3")}


def database_label() -> str:
    """A safe description of the database (never the password), shown in the app's banner."""
    db = DATABASES["default"]
    if db["ENGINE"].endswith("sqlite3"):
        return f"SQLite {Path(str(db['NAME'])).name}"
    engine = db["ENGINE"].rsplit(".", 1)[-1].replace("postgresql", "PostgreSQL").replace("mysql", "MySQL")
    return f"{engine} {db.get('HOST') or 'localhost'}/{db['NAME']}"


DATABASE_LABEL = database_label()


# Password validation
# https://docs.djangoproject.com/en/6.1/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/6.1/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/6.1/howto/static-files/

STATIC_URL = 'static/'


# Email
# https://docs.djangoproject.com/en/6.1/topics/email/#topic-email-configuration

MAILERS = {
    'default': {
        'BACKEND': 'django.core.mail.backends.console.EmailBackend',
    },
}


# ---- Django REST Framework -----------------------------------------------------
# Every API request must carry a login token ("Authorization: Token <key>"),
# except the login endpoint itself, which sets its own permission.
REST_FRAMEWORK = {
    "DEFAULT_AUTHENTICATION_CLASSES": ["rest_framework.authentication.TokenAuthentication"],
    "DEFAULT_PERMISSION_CLASSES": ["rest_framework.permissions.IsAuthenticated"],
    "DEFAULT_PAGINATION_CLASS": "rest_framework.pagination.PageNumberPagination",
    "PAGE_SIZE": 50,
}