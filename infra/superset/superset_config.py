import os


SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "change-me-before-sharing")
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"
WTF_CSRF_ENABLED = False
TALISMAN_ENABLED = False
ROW_LIMIT = 5000
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
}
