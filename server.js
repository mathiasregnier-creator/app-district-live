const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const sqlite3 = require('sqlite3').verbose();
const cron = require('node-cron');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static('public'));
app.use(express.json());

const db = new sqlite3.Database('./district.db', (err) => {
    if (err) console.error("Erreur DB:", err.message);
    else console.log("Base de données SQLite connectée.");
});

// Chronomètre serveur
let tempsEcoule = 0; // en secondes
let chronoTimer = null;

db.serialize(() => {
    db.run(`CREATE TABLE IF NOT EXISTS matchs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        equipe_dom TEXT,
        equipe_ext TEXT,
        score_dom INTEGER DEFAULT 0,
        score_ext INTEGER DEFAULT 0,
        statut TEXT DEFAULT 'Non démarré',
        temps INTEGER DEFAULT 0
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS joueurs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nom TEXT, prenom TEXT, numero INTEGER, poste TEXT,
        buts INTEGER DEFAULT 0, passes INTEGER DEFAULT 0
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS actions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        match_id INTEGER,
        minute INTEGER,
        texte TEXT,
        type TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);

    db.get("SELECT COUNT(*) as count FROM matchs", (err, row) => {
        if (row && row.count === 0) {
            db.run(`INSERT INTO matchs (equipe_dom, equipe_ext, score_dom, score_ext, statut, temps) 
                    VALUES ('AAS Cléry Mareau Dry', 'Équipe Adverse', 0, 0, 'À venir', 0)`);
        }
    });
});
// Synchro automatique tous les matins à 6h00
cron.schedule('0 6 * * *', () => {
    synchroniserMatchFFF();
});

function lancerChrono(matchId) {
    if (chronoTimer) clearInterval(chronoTimer);
    chronoTimer = setInterval(() => {
        tempsEcoule++;
        io.emit('tick_chrono', { temps: tempsEcoule });
    }, 1000);
}

function stopperChrono() {
    if (chronoTimer) {
        clearInterval(chronoTimer);
        chronoTimer = null;
    }
}

function enregistrerAction(matchId, texte, type) {
    const minuteActuelle = Math.floor(tempsEcoule / 60);
    db.run(`INSERT INTO actions (match_id, minute, texte, type) VALUES (?, ?, ?, ?)`,
        [matchId, minuteActuelle, texte, type],
        function(err) {
            if (!err) {
                db.all("SELECT * FROM actions WHERE match_id = ? ORDER BY id DESC", [matchId], (err, actions) => {
                    io.emit('mise_a_jour_fil', actions);
                });
            }
        }
    );
}

io.on('connection', (socket) => {
    console.log('Un utilisateur s\'est connecté');

    db.get("SELECT * FROM matchs ORDER BY id DESC LIMIT 1", (err, match) => {
        if (match) {
            match.temps = tempsEcoule;
            socket.emit('mise_a_jour_score', match);
            
            db.all("SELECT * FROM actions WHERE match_id = ? ORDER BY id DESC", [match.id], (err, actions) => {
                if (actions) socket.emit('mise_a_jour_fil', actions);
            });
        }
    });

    db.all("SELECT * FROM joueurs ORDER BY numero ASC", (err, joueurs) => {
        if (joueurs) socket.emit('mise_a_jour_joueurs', joueurs);
    });

    socket.on('modifier_score', (data) => {
        const champ = data.equipe === 'dom' ? 'score_dom' : 'score_ext';
        
        db.run(`UPDATE matchs SET ${champ} = MAX(0, ${champ} + ?) WHERE id = ?`, [data.delta, data.id], function(err) {
            if (!err) {
                db.get("SELECT * FROM matchs WHERE id = ?", [data.id], (err, match) => {
                    if (match) {
                        match.temps = tempsEcoule;
                        io.emit('mise_a_jour_score', match);

                        if (data.delta > 0) {
                            const nomEquipe = data.equipe === 'dom' ? match.equipe_dom : match.equipe_ext;
                            enregistrerAction(match.id, `⚽ BUT pour ${nomEquipe} ! (${match.score_dom} - ${match.score_ext})`, 'but');
                        }
                    }
                });
            }
        });
    });
    // Événement pour changer l'équipe adverse instantanément
    socket.on('changer_equipe_ext', (data) => {
        db.run(`UPDATE matchs SET equipe_ext = ? WHERE id = 1`, [data.nomAdversaire], function(err) {
            if (!err) {
                db.get("SELECT * FROM matchs WHERE id = 1", (err, match) => {
                    if (match) io.emit('mise_a_jour_score', match);
                });
            }
        });
    });

    socket.on('action_chrono', (data) => {
        let nouveauStatut = '';
        let messageAction = '';

        if (data.action === 'start') {
            nouveauStatut = '1ère Mi-temps';
            messageAction = '▶ Coup d\'envoi de la 1ère mi-temps !';
            lancerChrono(data.id);
        } else if (data.action === 'start2') {
            nouveauStatut = '2ème Mi-temps';
            messageAction = '▶ Début de la 2ème mi-temps !';
            lancerChrono(data.id);
        } else if (data.action === 'mitemps') {
            nouveauStatut = 'Mi-temps';
            messageAction = '⏸ Coup de sifflet : C\'est la mi-temps !';
            stopperChrono();
            tempsEcoule = 45 * 60;
            io.emit('tick_chrono', { temps: tempsEcoule });
        } else if (data.action === 'fin') {
            nouveauStatut = 'Terminé';
            messageAction = '🏁 Fin du match !';
            stopperChrono();
        }

        db.run(`UPDATE matchs SET statut = ?, temps = ? WHERE id = ?`, [nouveauStatut, tempsEcoule, data.id], function(err) {
            db.get("SELECT * FROM matchs WHERE id = ?", [data.id], (err, match) => {
                if (match) {
                    match.temps = tempsEcoule;
                    io.emit('mise_a_jour_score', match);
                    if (messageAction) {
                        enregistrerAction(match.id, messageAction, 'info');
                    }
                }
            });
        });
    });

    socket.on('ajouter_joueur', (joueur) => {
        db.run(`INSERT INTO joueurs (nom, prenom, numero, poste) VALUES (?, ?, ?, ?)`,
            [joueur.nom, joueur.prenom, joueur.numero, joueur.poste],
            function(err) {
                if (!err) {
                    db.all("SELECT * FROM joueurs ORDER BY numero ASC", (err, joueurs) => {
                        io.emit('mise_a_jour_joueurs', joueurs);
                    });
                }
            }
        );
    });

    socket.on('ajouter_stat_joueur', (data) => {
        const champ = data.type === 'buts' ? 'buts' : 'passes';
        db.run(`UPDATE joueurs SET ${champ} = ${champ} + 1 WHERE id = ?`, [data.joueur_id], function(err) {
            if (!err) {
                db.all("SELECT * FROM joueurs ORDER BY numero ASC", (err, joueurs) => {
                    io.emit('mise_a_jour_joueurs', joueurs);
                });
            }
        });
    });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
    console.log(`\n==================================================`);
    console.log(` Serveur démarré sur le port : ${PORT}`);
    console.log(`==================================================\n`);
});

    // Lancement de la recherche du match dès le démarrage
    synchroniserMatchFFF();
});
