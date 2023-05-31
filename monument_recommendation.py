import numpy as np
import pandas as pd
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
from lightfm.data import Dataset
from lightfm import LightFM


class Recommendation:

    def __init__(self, path_visit, path_item):
        self.df_visit = pd.read_csv(path_visit)
        self.df_item = pd.read_csv(path_item)
        self.user_dict = self.create_user_dict(name_col='user_id')
        self.item_dict = self.create_item_dict(id_col='mon_id', name_col='name')
        self.model = self.model_fit()
        matrices = self.interaction_feature_matrix()
        self.interaction = matrices[0]
        self.features = matrices[1]

    def create_user_dict(self, name_col):
        user_dict = {}
        counter = 0
        for i in self.df_visit[name_col].unique():
            user_dict[i] = counter
            counter += 1
        return user_dict

    def create_item_dict(self, id_col, name_col):
        item_dict = {}
        for i in range(self.df_item.shape[0]):
            item_dict[(self.df_item.loc[i, id_col])] = self.df_item.loc[i, name_col]
        return item_dict

    def create_feature_dict(self, features):
        dict_features = []
        for i, f, c in zip(self.df_item.mon_id.values, features, self.df_item.category.values):
            dict_features.append({'mon_id': i, 'description': f, 'category': c})
        return dict_features

    def get_descriptions_vec(self):
        tagged_desc = [TaggedDocument(doc, [i]) for i, doc in enumerate(self.df_item.description.values)]
        doc2vec_model = Doc2Vec(tagged_desc, vector_size=100, window=5, min_count=1, workers=4, epochs=20)
        desc_vectors = np.zeros((len(self.df_item.description.values), doc2vec_model.vector_size))
        for i in range(len(self.df_item.description.values)):
            desc_vectors[i] = doc2vec_model.dv[i]
        return desc_vectors

    def interaction_feature_matrix(self):
        desc_vec = self.get_descriptions_vec()
        dict_features = self.create_feature_dict(desc_vec)
        dataset = Dataset()
        dataset.fit(users=self.df_visit.user_id.values, items=self.df_item.mon_id.values)
        dataset.fit_partial(items=self.df_item.mon_id.values,
                            item_features=((tuple(x['description']), tuple(x['category'])) for x in dict_features))
        interactions = list(zip(self.df_visit.user_id, self.df_visit.monument_id))
        (interaction, weights) = dataset.build_interactions(interactions)
        item_features = dataset.build_item_features(
            ((x['mon_id'], [(tuple(x['description']), tuple(x['category']))]) for x in dict_features))
        return interaction, item_features

    def model_fit(self):
        (interaction, item_features) = self.interaction_feature_matrix()
        model = LightFM(loss='warp')
        model.fit(interaction, item_features=item_features)
        return model

    def recommendation(self, user_id):
        n_users, n_items = self.interaction.shape
        if user_id not in self.user_dict.keys():
            # cold start: recommend most popular items
            item_popularity = self.interaction.sum(axis=0).A1
            top_items = self.df_item['name'][np.argsort(-item_popularity)].values
            recommended_items = top_items[:3]
            print("User %s didn't have seen anything, we recommend the best monuments" % user_id)
            print("     Recommended:")
            for x in recommended_items:
                print("         %s" % x)

        else:
            user_x = self.user_dict[user_id]
            known_positives = set(self.df_item['name'][self.interaction.tocsr()[user_x].indices])
            scores = self.model.predict(user_x, np.arange(n_items), item_features=self.features)
            top_items = self.df_item['name'][np.argsort(-scores)].values
            recommended_items = []
            for item in top_items:
                if item not in known_positives:
                    recommended_items.append(item)
                if len(recommended_items) >= 3:
                    break

            print("User %s" % user_id)
            print("     Known positives:")
            for x in known_positives:
                print("         %s" % x)

            print("     Recommended:")
            for x in recommended_items:
                print("         %s" % x)

        return [x for x in recommended_items]
