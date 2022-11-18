###############
# pointcloud - odom message synchronization
# pointcloud world --> body transform
###############

import rosbag
import rospy
from geometry_msgs.msg import TransformStamped
from tf2_msgs.msg import TFMessage
import time
import copy
from sensor_msgs.point_cloud2 import read_points, create_cloud
import PyKDL


# specify pc and odom topics you want to synchronize
topic_name_pc = 'kalman_scan2map/cloud_local'
topic_name_odom = 'kalman_scan2map/odometry'
input_bag_path = 'data_test.bag'
output_bag_path = 'data_processed.bag'
freq = 10.0 # [Hz], float

def synchronize():

    # temp topic store
    tmp1 = []
    tmp2 = []
    rslt = []

    # synchronization
    bag = rosbag.Bag(input_bag_path)
    for topic, msg, t in bag.read_messages(topics=[topic_name_pc, topic_name_odom]):
        if topic == topic_name_pc:
            t = msg.header.stamp
            index = None
            for i, t2 in enumerate(tmp2):
                if abs(t2[0] - t) < rospy.Duration(0.001):
                    rslt.append((msg, t2[1]))
                    index = i
                    break
            if index == None:
                tmp1.append((msg.header.stamp, msg))
            else:
                del tmp2[index]

        elif topic == topic_name_odom:
            t = msg.header.stamp
            index = None
            for i, t1 in enumerate(tmp1):
                if abs(t1[0] - t) < rospy.Duration(0.001):
                    rslt.append((t1[1], msg))
                    index = i
                    break
            if index == None:
                tmp2.append((msg.header.stamp, msg))
            else:
                del tmp1[index]
        
        else:
            print("Warning: unknown topic")

    bag.close()

    # examinate the result
    print(len(rslt))
    for r in rslt:
        print(r[0].header.stamp, r[1].header.stamp)

    print("-------------Residual-------------")
    if not len(tmp1)*len(tmp2):
        print("All data synchronized!")
    else:
        for q1 in tmp1:
            print(q1[0])
        print("----------------------")
        for q2 in tmp2:
            print(q2[0])

    return rslt


# pointcloud2 transform algorithm
def transform_cloud(cloud_in, t):
    p_odom = PyKDL.Vector(t.transform.translation.x, t.transform.translation.y, t.transform.translation.z)
    q_odom_inv = PyKDL.Rotation.Quaternion(-t.transform.rotation.x, -t.transform.rotation.y, -t.transform.rotation.z, t.transform.rotation.w)
    points_out = []
    for p_in in read_points(cloud_in):
        p_point = PyKDL.Vector(p_in[0], p_in[1], p_in[2])
        p_out = q_odom_inv*(p_point - p_odom)
        points_out.append((p_out[0], p_out[1], p_out[2]) + p_in[3:])
    res = create_cloud(t.header, cloud_in.fields, points_out)
    return res


# transform and republish
def transform_process(rslt):

    bag = rosbag.Bag(output_bag_path,'w')
    t = rospy.Time.from_sec(time.time())
    for r in rslt:
        pc = r[0]
        odom = r[1]

        trans = TransformStamped()
        trans.header = odom.header
        trans.child_frame_id = odom.child_frame_id
        trans.transform.translation.x = odom.pose.pose.position.x
        trans.transform.translation.y = odom.pose.pose.position.y
        trans.transform.translation.z = odom.pose.pose.position.z
        trans.transform.rotation = odom.pose.pose.orientation

        pc_body = transform_cloud(pc, copy.deepcopy(trans))
        pc_body.header.frame_id = odom.child_frame_id

        tf = TFMessage()
        tf.transforms.append(trans)

        try:
            bag.write(topic_name_pc, pc, t)
            bag.write(topic_name_odom, odom, t)
            bag.write('point_cloud_body', pc_body, t)
            bag.write('trans_stamped', trans, t)
            bag.write('tf', tf, t)
        except:
            print("Error in bag writing")
            break

        t += rospy.Duration(1/freq)
    bag.close()


def main():

    rslt = synchronize()
    transform_process(rslt)


if __name__ == "__main__":
    main()