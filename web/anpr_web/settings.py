

import os
import sys
from pathlib import Path

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = BASE_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))  # lets Django import the pipeline package

# Local secrets and machine-specific settings (database login etc.) live in main/.env,
# never in the code. See main/.env.example.
from dotenv import load_dotenv  # noqa: E402
load_dotenv(PROJECT_ROOT / ".env")

# Which environment this is (local / staging / production). Services pass --env to set it.
ANPR_ENV = os.environ.get("ANPR_ENV", "local")
# ANPR_READ_ONLY=1: look but don't change (e.g. pointing a local copy at production data).
READ_ONLY = os.environ.get("ANPR_READ_ONLY", "").lower() in ("1", "true", "yes")


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/6.1/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'django-insecure-i-!+pwez7g2#n1gq*+43_srz)e17!+l!&ggi#wk)d#3#up4m^o'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = []


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

# ANPR_DB_ENGINE=postgres  -> PostgreSQL (settings from POSTGRES_* in main/.env)
# anything else / unset    -> SQLite file web/db.sqlite3 (the default)
if os.environ.get("ANPR_DB_ENGINE", "sqlite").lower() in ("postgres", "postgresql"):
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.postgresql',
            'NAME': os.environ.get('POSTGRES_DB', 'anpr'),
            'USER': os.environ.get('POSTGRES_USER', 'anpr'),
            'PASSWORD': os.environ.get('POSTGRES_PASSWORD', 'your-password'),
            'HOST': os.environ.get('POSTGRES_HOST', 'localhost'),
            'PORT': os.environ.get('POSTGRES_PORT', '5432'),
            'CONN_MAX_AGE': 60,              # reuse connections: bots query every second
            'CONN_HEALTH_CHECKS': True,      # drop a dead connection instead of erroring
        }
    }
else:
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': os.environ.get('ANPR_DB_PATH', BASE_DIR / 'db.sqlite3'),
            'OPTIONS': {
                'timeout': 20,                       # wait for a busy database instead of failing
                'transaction_mode': 'IMMEDIATE',         # take the write lock up front: fewer 'database is locked' errors
                'init_command': 'PRAGMA journal_mode=WAL;',  # readers don't block writers
            },
        }
    }


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