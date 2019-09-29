FROM node:10.16 AS build-client
# Build the client of the game
WORKDIR /app

COPY ./agot-bg-game-server/package.json . 
RUN yarn install

COPY ./agot-bg-game-server/ .

RUN yarn run generate-json-schemas
RUN yarn run build-client

FROM python:3.6

WORKDIR /app

COPY ./agot-bg-website/requirements.txt .
RUN pip install -r requirements.txt
RUN pip install gunicorn==19.9.0

# From the previous stage, copy the assets and the index.html
COPY --from=build-client /app/dist ./static_game
COPY --from=build-client /app/dist/index.html ./agotboardgame_main/templates/agotboardgame_main/play.html

COPY ./agot-bg-website .

RUN SECRET_KEY=not_used DATABASE_URL=not_used python manage.py collectstatic

RUN mkdir /django_metrics

CMD gunicorn agotboardgame.wsgi:application -c gunicorn_config.py --bind 0.0.0.0:$PORT --workers 2