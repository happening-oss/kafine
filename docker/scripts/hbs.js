#!/usr/bin/env node

const Handlebars = require("handlebars");
const fs = require('fs');
const yaml = require('yaml');

const propertiesFileName = process.argv[2];
const templateFileName = process.argv[3];

const template = Handlebars.compile(fs.readFileSync(templateFileName, 'utf8'));
const properties = yaml.parse(fs.readFileSync(propertiesFileName, 'utf8'));

console.log(template(properties));
