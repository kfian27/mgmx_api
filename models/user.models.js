const bcrypt = require("bcrypt");

module.exports = (sequelize, Sequelize) => {
    var User = sequelize.define(
        "user", {
            id: {
                type: Sequelize.INTEGER,
                autoIncrement: true,
                primaryKey: true,
            },
            email: {
                type: Sequelize.STRING,
            },
            password: {
                type: Sequelize.STRING,
            },
            salt: {
                type: Sequelize.STRING,
            },
            nama: {
                type: Sequelize.STRING,
            },
            last_login: {
                type: Sequelize.DATE,
            },
        }, {
            tableName: "user",
            createdAt: "created_at",
            updatedAt: "updated_at",
            deletedAt: "delete_at",
            paranoid: true,
            timestamps: true,
        }
    );

    User.prototype.setPassword = async function() {
        let arr = {};
        arr.salt = bcrypt.genSaltSync(10);

        arr.password = await bcrypt.hash(this.password, arr.salt);

        return arr;
    };

    User.prototype.verifyPassword = async function(password) {
        let cek = bcrypt.compare(password, this.password);
        return cek;
    };

    return User;
};