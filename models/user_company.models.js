module.exports = (sequelize, Sequelize) => {
    var User_Company = sequelize.define(
        "user_company", {
            id: {
                type: Sequelize.INTEGER,
                autoIncrement: true,
                primaryKey: true,
            },
        }, {
            tableName: "user_company",
            createdAt: "created_at",
            updatedAt: "updated_at",
            // deletedAt: "delete_at",
            paranoid: false,
            timestamps: true,
        }
    );

    return User_Company;
};