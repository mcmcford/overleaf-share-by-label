# This project aims to provide individuals a way of sharing Overleaf projects in Overleaf CE with everyone in their team by simply using tags

## Concept

1. This script will be given a preset list of tags to look for in the tags table of the mongoDB of overleaf CE.
These tags will be passed in via an environment variable called `enforced_tags`. If any overleaf user as defined in the overleaf users table does not have these tags associated with their account, this script will forcefully add these tags to their account.

Each team will have multiple tags associated with it, for example team "Threat Intelligence" will have the tags "TI - Collaborator", "TI - Reviewer" and "TI - Read Only" associated with it, this allows the creator of a project to define what level of access a team should have to their project.

2. The script, will be capable of associating users to given teams through an authentik integration. Each tag created in the previous step will be associated with a group in authentik. the association between users in authentik and overleaf will ideally be done using using the samlIdentifiers[0].externalUserId field in the overleaf users table however if that can't be correlated to anything in authentik, then we can fall back to using the email field in the overleaf users table and the email field in authentik.

3. When the owner of a project as defined by owner_ref in the project records of the mongoDB of overleaf CE adds one of the predefined tags to their project, the script will pick this up on its next run and compare the collaberator_refs, reviewer_refs and readOnly_refs fields to the groups of users it pulls from authentik. If someone has added "TI - Read Only" to their project then it will look for all members of the Threat Intelligence team in authentik and add them to the readOnly_refs field of the project, if someone has added "TI - Collaborator" to their project then it will look for all members of the Threat Intelligence team in authentik and add them to the collaberator_refs field of the project and so on. 

If the owner of the project is in the team, it will ignore them as they will always have full access to the project.

If the owner adds two different levels of access for the same team, it will remove the lower level of access, for example if the owner adds "TI - Collaborator" and "TI - Read Only" to their project, it will remove all members of the Threat Intelligence team from the readOnly_refs field and add them to the collaberator_refs field, as well as removing the "TI - Read Only" tag from the project.

4. If the owner of the project removes one of the predefined tags from their project, the script will pick this up on its next run and remove all users associated with that tag from the project, for example if the owner removes "TI - Collaborator" from their project, it will look for all members of the Threat Intelligence team in authentik and remove them from the collaberator_refs field of the project.

## Docker

This repository includes a `Dockerfile` that packages the sync script as a Python container image.

Build the image:

```bash
docker build -t overleaf-teams:local .
```

Run it with environment variables:

```bash
docker run --rm \
	-e AUTHENTIK_URL="https://authentik.example.com" \
	-e AUTHENTIK_TOKEN="replace-me" \
	-e OVERLEAF_MONGO_DB="sharelatex" \
	-e OVERLEAF_MONGO_URI="mongodb://mongo:27017/" \
	-e TEAMS="Threat Intelligence,Platform Engineering" \
	-e CREATE_GROUPS="true" \
	-e CREATE_TAGS="false" \
	-e APPLY_PROJECT_ACCESS="false" \
	overleaf-teams:local
```

Required environment variables:

- `AUTHENTIK_URL`
- `AUTHENTIK_TOKEN`
- `OVERLEAF_MONGO_DB`
- `TEAMS`

MongoDB connection options:

- Set `OVERLEAF_MONGO_URI` directly, or
- Provide `OVERLEAF_MONGO_HOST`, `OVERLEAF_MONGO_PORT`, `OVERLEAF_MONGO_USERNAME`, `OVERLEAF_MONGO_PASSWORD`, `OVERLEAF_MONGO_AUTH_DB`, `OVERLEAF_MONGO_AUTH_MECHANISM`, and `OVERLEAF_MONGO_TLS`.

Useful optional variables:

- `AUTHENTIK_PAGE_SIZE`
- `CREATE_GROUPS`
- `CREATE_TAGS`
- `APPLY_PROJECT_ACCESS`
- `OVERLEAF_TAG_COLOR`

## Helm

The chart in `charts/overleaf-teams` deploys the application as a Kubernetes `CronJob`, which fits this script better than a long-running deployment.

Create a values file, for example `values.yaml`:

```yaml
image:
	repository: ghcr.io/your-org/overleaf-teams
	tag: "latest"

config:
	authentikUrl: "https://authentik.example.com"
	overleafMongoDb: "sharelatex"
	teams: "Threat Intelligence,Platform Engineering"
	createGroups: "true"
	createTags: "false"
	applyProjectAccess: "false"

secret:
	authentikToken: "replace-me"
	overleafMongoUri: "mongodb://mongo.mongodb.svc.cluster.local:27017/"
```

Install the chart:

```bash
helm upgrade --install overleaf-teams ./charts/overleaf-teams -f values.yaml
```

If you prefer to use an existing Kubernetes secret instead of storing secrets in Helm values, set `existingSecret` and make sure it exposes these keys:

- `authentik-token`
- `overleaf-mongo-uri`
- `overleaf-mongo-username`
- `overleaf-mongo-password`

The default schedule is every 15 minutes and can be changed with `cronjob.schedule`.