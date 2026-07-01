import ModuleAlias from 'module-alias'

ModuleAlias.addAliases({
  '@apps': `${__dirname}/apps`,
  '@core': `${__dirname}/core`,
  '@config': `${__dirname}/config`,
  '@routes': `${__dirname}/routes`,
  '@locales': `${__dirname}/locales`,
  '@middlewares': `${__dirname}/middlewares`,
  '@prisma': `${__dirname}/../../bf-exchange/prisma/generated/prisma-exchange`,
})
