from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations
from math import sqrt

cols = 'id,verified,profile_sidebar_fill_color,profile_text_color,followers_count,protected,location,profile_background_color,utc_offset,statuses_count,description,friends_count,profile_link_color,profile_image_url,notifications,profile_background_image_url,screen_name,profile_background_tile,favourites_count,name,url,created_at,time_zone,profile_sidebar_border_color,following,gender (infered by name)'.split(',')


class userSimilarities_onCelebrities(MRJob):

	def group_by_different_keywords(self, _ , info):
		"""
		emit the key words and group by their values
		followers_count 48833,333333
		followers_count 48834,233122
		followers_count 48835,124442

		statues_count 48833,5600
		statues_count 48834,3221
		statues_count 48835,2213

		"""
		row = dict(zip(cols,info.split('\t')))
		keywords = ['followers_count','statuses_count','friends_count','favourites_count']
		for k in keywords:
			yield k,(int(row['id']),int(row[k]))

	def count_values_keywords_freq(self, keyword, values):
		"""
		For each keyword, emit  a row containing their realtive info
		(user, values pair)
		followers_count 48833,333333

        	we can count the number of values for futher use, but this time I do not use it 

		"""
        	count = 0
        	final = []
        	for i in values:
            		count += 1
            		final.append(i)
		yield keyword,(count,final)

	def pairwise_user(self, keyword, values):
		"""
		the output drops the keywords from the key entirely, 
        	instead it emits the pair of users as the key:

		48833,48834 333333,233122
		48833,48834 5600,3221
		
		it is said that this part is the main bottleneck of the main performance,
		but i haven't figured it out for the time being
		"""
		count, value = values
		for v1,v2 in combinations(value,2):
			yield (v1[0],v2[0]),(v1[1],v2[1])

	def calculate_similarity(self, pair_user, lines):
		"""
		sum components of each user pair across all keywords
		then calculate pairwise person similarity
		the similarities are normalized to the [0,1] scale

		48833,48834 0.4 
		48833,48835 0.6
		"""

		sum_xx,sum_xy,sum_yy,sum_x,sum_y,n = (0.0,0.0,0.0,0.0,0.0,0)
		n_x, n_y = 0, 0
		(user_x,user_y), co_values = pair_user, lines
		for v_x,v_y in lines:
			sum_xx += v_x**2
			sum_yy += v_y**2
			sum_xy += v_x*v_y
			sum_y += v_y
			sum_x += v_x
        		n+=1

		pearson = (n * sum_xy - sum_x*sum_y)/(sqrt(n*sum_xx-sum_x**2)*sqrt(n*sum_yy-sum_y**2))

        	corr_sim = pearson*0.5 + 0.5

		yield (user_x,user_y),corr_sim


  	def calculate_ranking(self,pair_user,v):
        	user_x,user_y = pair_user
       	 	yield user_x,(user_y,v)
        	yield user_y,(user_x,v)

	def top_similar_celebrities(self, user, values):
        	topFive = []
        	for (u,v) in values:
            		topFive.append((u,v))
            		topFive.sort(key = lambda x: (-x[1],x[0]))
            		topFive = topFive[:5]

        	for (u,v) in topFive:
            		yield user,(u,v)


	def steps(self):
        	return [MRStep(mapper = self.group_by_different_keywords,reducer = self.count_values_keywords_freq), MRStep(mapper= self.pairwise_user, reducer=self.calculate_similarity),MRStep(mapper=self.calculate_ranking, reducer=self.top_similar_celebrities)]

if __name__ == '__main__':
    userSimilarities_onCelebrities.run()
