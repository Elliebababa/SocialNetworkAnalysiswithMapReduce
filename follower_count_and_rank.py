from mrjob.job import MRJob
from mrjob.step import MRStep 
class FollowerCountRank(MRJob):
	
	"""
	a mapreduce job to count the number of followers each user has
	and rank according to the number 

	input 
		<user_id>\t<follower_id>
		e.g.
		12	31
		12	38
		12	41
		12	47

	output
		<1st_user_id>\t<#follwers of the user>
		<2nd_user_id>\t<#follwers of the user>
		...
		e.g
		12 34982
		47 23434
		...
	"""

	"""
	map reduce step 1
	read the data and count the number of the user
	
	map:
	record of the each follower of a user, output: <user_id>/t<1>
		12 1
		12 1
		12 1

		13 1
		13 1 

	reduce:sum the followers of each user,output: <user_id>/t<#followers>
		12 3
		13 2

	"""
	def calFollowers(self,key,line):
		yield int(line.split('\t')[0]),1

	def sumFolowers(self,id,followers):
		s = 0
		for f in followers:
			s += f
		yield id,s

	"""
	map reduce step 2
	rank the user according to the number of the followers, for those who have the same number of followers, rank them in the order of user_id
	notice that in order to rank the user, I map the user into the same reducer with all their key set to be None

	map:
	record of the each follower of a user, output: <user_id>/t<1>
		

	reduce:sum the followers of each user,output: <user_id>/t<#followers>
		12 3
		13 2

	"""
	def s2map(self,id,followers_count):
		yield None,(id,followers_count)

	def selectTop(self,_,value):
		topTen = []
		for (u,followers_count) in value:
			topTen.append((u,followers_count))
			topTen.sort(key = lambda x:(-x[1],x[0]))
			topTen = topTen[:10]
		for p in topTen:
			yield p[0],p[1]


	def steps(self):
		return [MRStep(mapper = self.calFollowers, reducer = self.sumFolowers),
		MRStep(mapper = self.s2map, reducer = self.selectTop)]

if __name__ == '__main__':
	instance = FollowerCountRank()
	instance.run()
