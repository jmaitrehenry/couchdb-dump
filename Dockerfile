FROM node:9-alpine

COPY . /code
WORKDIR /code
RUN npm i

ENV PATH="/code/bin:$PATH"
CMD ["node", "cdbdump"]
