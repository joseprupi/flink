package org.apache.flink.examples.java.ap;

import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.examples.java.ap.util.AffinityPropagationData;
import org.apache.flink.types.BooleanValue;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * Created by joseprubio on 9/22/16.
 */

public class AffinityPropagationBulk {

	public static void main(String[] args) throws Exception {

		//ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		// Get input similarities Tuple3<src, target, similarity>
		DataSet<Tuple3<LongValue, LongValue, DoubleValue>> similarities =
			AffinityPropagationData.getTuples(env);

		// Init input to iteration
		DataSet<Tuple6<LongValue, LongValue, DoubleValue, DoubleValue, BooleanValue, IntValue>> initMessages
			= similarities.map(new InitMessage());

		// Iterate
		IterativeDataSet<Tuple6<LongValue, LongValue, DoubleValue, DoubleValue, BooleanValue, IntValue>> messages
			= initMessages.iterate(15);

		// Start responsibility message calculation
		// r(i,k) <- s(i,k) - max {a(i,K) + s(i,K)} st K != k
		// Iterate over Tuple6 <Source, Target, Responsibility , Availavility, IsExemplar, ConvergenceCounter>

		//DataSet<Message> responsibilities = similarities
		DataSet<Tuple6<LongValue, LongValue, DoubleValue, DoubleValue, BooleanValue, IntValue>> responsibilities = similarities

			// Get a list of a(i,K) + s(i,K) values joining similarities with messages
			.join(messages)
			.where("f1","f0")
			.equalTo("f1","f0")
			.with(new JoinFunction<	Tuple3<LongValue, LongValue, DoubleValue>,
				Tuple6<LongValue, LongValue, DoubleValue, DoubleValue, BooleanValue, IntValue>,
				Tuple3<LongValue, LongValue, DoubleValue>>() {
				public Tuple3<LongValue, LongValue, DoubleValue> join(Tuple3<LongValue, LongValue, DoubleValue> left,
					Tuple6<LongValue, LongValue, DoubleValue, DoubleValue, BooleanValue, IntValue> right) throws Exception {
					return new Tuple3<>(left.f0, left.f1,
						new DoubleValue(left.f2.getValue() + right.f2.getValue()));
				}
			})

			// Get a dataset with 2 higher values
			.groupBy("f1").sortGroup("f2", Order.DESCENDING)
			.first(2)

			// Create a Tuple4<Trg, MaxValue, MaxNeighbour, SecondMaxValue> reducing the 2 tuples with higher values
			.groupBy("f1")
			.reduceGroup(new responsibilityReduceGroup())

			// Calculate the R messages "r(i,k) <- s(i,k) - value" getting "value" joining
			// similarities with previous tuple
			.leftOuterJoin(similarities)
			.where("f0")
			.equalTo("f1")
			.with(new responsibilityValue());

		// Start availability message calculation
		// a(i,k) <- min {0, r(k,k) + sum{max{0,r(I,k)}} I st I not in {i,k}
		// a(k,k) <- sum{max{0,r(I,k)} I st I not in {i,k}

		DataSet<Tuple6<LongValue, LongValue, DoubleValue, DoubleValue, BooleanValue, IntValue>> availabilities = responsibilities

			// Get the sum of the positive responsibilities and the self responsibility per target
			.groupBy("f1")
			.reduceGroup(new availabilityReduceGroup())

			// Calculate the availability
			.leftOuterJoin(responsibilities)
			.where("f0")
			.equalTo("f1")
			.with(new availabilityValue());

		// End iteration
		DataSet<Tuple6<LongValue, LongValue, DoubleValue, DoubleValue, BooleanValue, IntValue>> finalMessages =
			messages.closeWith(availabilities);

		for(Tuple6<LongValue, LongValue, DoubleValue, DoubleValue, BooleanValue, IntValue> m : finalMessages.collect()){
			System.out.println("Final -> Src: " + m.f0.getValue() + " Trg: " + m.f1.getValue() + " A: " + m.f2.getValue() + " R: "
				+ m.f4.getValue());
		}

	}

	private static class InitMessage implements MapFunction<Tuple3<LongValue, LongValue, DoubleValue>,
		Tuple6<LongValue, LongValue, DoubleValue, DoubleValue, BooleanValue, IntValue>> {
		@Override
		public Tuple6<LongValue, LongValue, DoubleValue, DoubleValue, BooleanValue, IntValue>
		map(Tuple3<LongValue, LongValue, DoubleValue> in) {
			return new Tuple6<>(in.f0, in.f1, new DoubleValue(0.0), new DoubleValue(0.0), new BooleanValue(false), new IntValue(0));
		}
	}

	private static class ExemplarConvergence
		implements ConvergenceCriterion<DoubleValue> {
		private double convergenceThreshold;

		public ExemplarConvergence(double convergenceThreshold) {
			this.convergenceThreshold = convergenceThreshold;
		}

		@Override
		public boolean isConverged(int iteration, DoubleValue value) {
			double val = value.getValue();
			return (0 <= val && val <= convergenceThreshold);
		}
	}

	// Create a Tuple4<Trg, MaxValue, MaxNeighbour, SecondMaxValue> reducing the 2 tuples with the max values
	private static class responsibilityReduceGroup
		implements GroupReduceFunction<	Tuple3<LongValue, LongValue, DoubleValue>,
		Tuple4<LongValue, DoubleValue, LongValue, DoubleValue>> {

		@Override
		public void reduce(Iterable<Tuple3<LongValue, LongValue, DoubleValue>> maxValues,
						   Collector<Tuple4<LongValue, DoubleValue, LongValue, DoubleValue>> out) throws Exception {

			Long maxNeighbour = Long.valueOf(0);
			Long trg = Long.valueOf(0);
			double maxValue = 0;
			double secondMaxValue = 0;

			for (Tuple3<LongValue, LongValue, DoubleValue> val : maxValues) {
				if(val.f2.getValue() > maxValue){
					secondMaxValue = maxValue;
					maxValue = val.f2.getValue();
					maxNeighbour = val.f0.getValue();
					trg = val.f1.getValue();
				}else{
					secondMaxValue = val.f2.getValue();
				}
			}

			Tuple4<LongValue, DoubleValue, LongValue, DoubleValue> result = new Tuple4<>();
			result.f0 = new LongValue(trg);
			result.f1 = new DoubleValue(maxValue);
			result.f2 = new LongValue(maxNeighbour);
			result.f3 = new DoubleValue(secondMaxValue);

			out.collect(result);

		}
	}

	// Subtract each respo
	private static class responsibilityValue
		implements JoinFunction<Tuple4<LongValue, DoubleValue, LongValue, DoubleValue>,
		Tuple3<LongValue, LongValue, DoubleValue>,
		Tuple6<LongValue, LongValue, DoubleValue, DoubleValue, BooleanValue, IntValue>> {

		//Receives Tuple4<Trg, MaxValue, MaxNeighbour, SecondMaxValue> and Tuple3<src, target, similarity>
		@Override
		public Tuple6<LongValue, LongValue, DoubleValue, DoubleValue, BooleanValue, IntValue>
		join(Tuple4<LongValue, DoubleValue, LongValue, DoubleValue> maxValues,
			 Tuple3<LongValue, LongValue, DoubleValue> similarity) {

			double responsibility = 0;

			if(similarity.f0.getValue() == maxValues.f2.getValue()){
				responsibility = similarity.f2.getValue() - maxValues.f3.getValue();
			}else{
				responsibility = similarity.f2.getValue() - maxValues.f1.getValue();
			}

			System.out.println("R -> Src: " + similarity.f1 + " Trg: " + similarity.f0 + " Value:" + responsibility);

			return new Tuple6<>(similarity.f1, similarity.f0, new DoubleValue(0.0),
								new DoubleValue(responsibility), new BooleanValue(false), new IntValue(0));
		}
	}

	// Return a Tuple3<Trg, PositiveResponsibilitiesAccumulator, SelfResponsibility>
	private static class availabilityReduceGroup
		implements GroupReduceFunction<Tuple6<LongValue, LongValue, DoubleValue, DoubleValue, BooleanValue, IntValue>,
		Tuple3<LongValue, DoubleValue, DoubleValue>> {

		@Override
		public void reduce(Iterable<Tuple6<LongValue, LongValue, DoubleValue, DoubleValue, BooleanValue, IntValue>> messages,
						   Collector<Tuple3<LongValue, DoubleValue, DoubleValue>> out) throws Exception {

			double accum = 0;
			double selfResponsibility = 0;
			Long trg = Long.valueOf(0);

			for (Tuple6<LongValue, LongValue, DoubleValue, DoubleValue, BooleanValue, IntValue> m : messages) {
				if(m.f0.getValue() == m.f1.getValue()){
					selfResponsibility = m.f3.getValue();
					trg = m.f1.getValue();
				}else{
					if(m.f3.getValue() > 0){
						accum = accum + m.f3.getValue();
					}
				}
			}

			Tuple3<LongValue, DoubleValue, DoubleValue> result = new Tuple3<>();
			result.f0 = new LongValue(trg);
			result.f1 = new DoubleValue(accum);
			result.f2 = new DoubleValue(selfResponsibility);

			out.collect(result);

		}
	}

	// Joins a Tuple3<Trg, PositiveResponsibilitiesAccumulator, SelfResponsibility> from previous step
	// and the responsibilities. For each responsibility will calculate the availability to be sent to the
	// responsibility source. In case of self availability will calculate the convergence too.
	private static class availabilityValue
		implements JoinFunction<Tuple3<LongValue, DoubleValue, DoubleValue>,
		Tuple6<LongValue, LongValue, DoubleValue, DoubleValue, BooleanValue, IntValue>,
		Tuple6<LongValue, LongValue, DoubleValue, DoubleValue, BooleanValue, IntValue>> {

		@Override
		public Tuple6<LongValue, LongValue, DoubleValue, DoubleValue, BooleanValue, IntValue>
		join(Tuple3<LongValue, DoubleValue, DoubleValue> first,
		Tuple6<LongValue, LongValue, DoubleValue, DoubleValue, BooleanValue, IntValue> message) throws Exception {

			Tuple6<LongValue, LongValue, DoubleValue, DoubleValue, BooleanValue, IntValue> availability = new Tuple6<>();
			availability.f0 = message.f1;
			availability.f1 = message.f0;
			availability.f3 = message.f3;
			availability.f4 = message.f4;
			availability.f5 = message.f5;

			//Take the responsibility value in case is positive, it will be subtracted to the positive accumulator later
			if(message.f3.getValue() > 0) {
				availability.f2 = new DoubleValue(message.f3.getValue());
			}else{
				availability.f2 = new DoubleValue(0);
			}

			//For self availability subtract the responsibility message and calculate the convergence
			if(message.f1.getValue() == message.f0.getValue()){
				//availability.f2 = new DoubleValue(first.f1.getValue() - availability.f2.getValue());
				availability.f2 = new DoubleValue(first.f1.getValue());

				//Calculate convergence with self availability and self responsibility
				if(	first.f2.getValue() + availability.f2.getValue() > 0 && availability.f4.getValue() ||
					first.f2.getValue() + availability.f2.getValue() < 0 && !availability.f4.getValue()){
					availability.f5.setValue(availability.f5.getValue() + 1);
				}else{
					availability.f5.setValue(0);
					availability.f4.setValue(!availability.f4.getValue());
				}

			}else{
				availability.f2 = new DoubleValue(Math.min(0, first.f1.getValue() - availability.f2.getValue() + first.f2.getValue()));
			}

			System.out.println("A -> Src: " + availability.f0 + " Trg: " + availability.f1 + " Value:" + availability.f2);

			return availability;
		}
	}
}
