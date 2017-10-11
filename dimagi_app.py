import os
import sys

_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(_root)

from app.dimagi_challenge_app import create_app


application = create_app()


if __name__ == '__main__':
    print "== Running in debug mode =="
    application.run(host='0.0.0.0', port=8072, debug=True)
