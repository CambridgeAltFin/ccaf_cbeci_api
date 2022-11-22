import os
import firebase_admin
from firebase_admin import credentials


def init_app(cert, options=None, *args, **kwargs):
    cred = credentials.Certificate(cert)

    if options is None:
        options = {
            'databaseURL': os.environ.get("FIREBASE_DATABASE_URL")
        }

    # return firebase_admin.initialize_app(cred, options, *args, **kwargs)


class Collections:
    """
    This enum represents collections in the firestore database.
    """
    TEXT_PAGES = 'TextPages'
    REPORTS = 'Reports'
    SPONSORS = 'Sponsors'
