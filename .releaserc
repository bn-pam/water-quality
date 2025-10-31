module.exports = {
  // Indique à Semantic Release de travailler sur la branche principale ('main')
  branches: ["main"],

  plugins: [
    // 1. Analyse les messages de commit selon Conventional Commits
    [
      "@semantic-release/commit-analyzer",
      {
        preset: "angular",
        // Règles personnalisées pour que 'refactor' et 'perf' déclenchent un patch, par exemple
        releaseRules: [
          { type: "refactor", release: "patch" },
          { type: "perf", release: "patch" }
        ],
      },
    ],
    // 2. Génère le contenu des notes de version
    ["@semantic-release/release-notes-generator"],

    // 3. Crée le fichier CHANGELOG.md
    ["@semantic-release/changelog", {
        "changelogFile": "CHANGELOG.md"
    }],

    // 4. Met à jour la version dans le fichier package.json
    ["@semantic-release/npm", {
      "npmPublish": false, // On n'utilise pas npm pour la publication
      "pkgRoot": "."
    }],

    // 5. Commit les fichiers mis à jour (CHANGELOG.md, package.json) et crée le tag Git
    [
      "@semantic-release/git",
      {
        "assets": ["package.json", "CHANGELOG.md"],
        "message": "chore(release): ${nextRelease.version} [skip ci]\n\n${nextRelease.notes}"
      }
    ],

    // 6. Publie la release sur GitHub
    ["@semantic-release/github"]
  ]
};