from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations
from math import sqrt

class SemicolonValueProtocol(object):
    # the ouput fotmat protocol
    def write(self, key, values):
        return key,'  recommandId:',values[0],'commonFriends:',values[1]

class userSimilarities(MRJob):

    #OUTPUT_PROTOCOL = SemicolonValueProtocol

    def group_by_different_followers(self, key ,line):
        """
        emit followers and group by their following
        <folloer_id>\t<user_id>
        31 12
        31 18

        38 12
        38 19

        """
        user,follower = line.split('\t')
	#print(user,follower)
        yield int(follower),int(user)

    def count_followers_freq(self, follower, values):
        """
        count how many does every follower follow
        also emit their following id for later step

        <follower_id>\t <#following>,[following_id,..]
        """
	count = 0
	following = []
	for i in values:
		count += 1
		following.append(i)
	#print follower,(count,following)
        yield follower,(count,following)


    def pairwise_user(self, keyword, values):
        """
        the output drops the keywords from the followers entirely, 
        instead it emits the pair of users as the key:
        
        <user_id1>,<user_id2>\t<1>
        12,13 1
        14,15 1

        it means user_id1 and user_id2 are followered by the same person at the same time 
        
        """
        following_count, followings = values
        for u1,u2 in combinations(followings,2):
            yield (u1,u2),following_count

    def addup_similarity(self, pair_user, lines):
        """
        count common followers
        """
        user_x,user_y = pair_user
        total = 0
        for i in lines:
            total += i

        yield (user_x,user_y),total

    def calculate_ranking(self,pair_user,v):

        user_x,user_y = pair_user
        yield user_x,(user_y,v)
        yield user_y,(user_x,v)
        

    def top_similar_users(self, user, values):
        topFive = []
        for (u,v) in values:
            topFive.append((u,v))
            topFive.sort(key = lambda x: (-x[1],x[0]))
            topFive = topFive[:5]

        for (u,v) in topFive:
            yield user,(u,v)


    def steps(self):
        return [MRStep(mapper = self.group_by_different_followers,reducer = self.count_followers_freq ), 
        MRStep(mapper= self.pairwise_user, reducer=self.addup_similarity),
        MRStep(mapper=self.calculate_ranking, reducer=self.top_similar_users)]

if __name__ == '__main__':
    userSimilarities.run()
